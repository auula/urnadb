// Copyright 2022 Leon Ding <ding_ms@outlook.com> https://urnadb.github.io

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vfs

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/golang/snappy"
)

var (
	AESCryptor       = new(Cryptor)
	SnappyCompressor = new(Snappy)
)

const (
	// 使用整数位标志存储状态
	EnabledEncryption  = 1 << iota // 1: 0001
	EnabledCompression             // 2: 0010
)

// 压缩和解密应该针对数据的 VALUE ? 部分进行压缩，这里针对的是不定长部分进行压缩和解密
// | DEL 1 | KIND 1 | EAT 8 | CAT 8 | KLEN 4 | VLEN 4 | KEY ? | VALUE ? | CRC32 4 |
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

type Encryptor interface {
	Encrypt(secret, plianttext []byte) ([]byte, error)
	Decrypt(secret, ciphertext []byte) ([]byte, error)
}

type Pipeline struct {
	Encryptor
	Compressor
	flags  int
	secret []byte
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		flags:      0,
		Encryptor:  nil,
		Compressor: nil,
	}
}

func (p *Pipeline) EnableEncryption() {
	p.flags |= EnabledEncryption
}
func (p *Pipeline) EnableCompression() {
	p.flags |= EnabledCompression
}

func (p *Pipeline) DisableEncryption() {
	p.flags &^= EnabledEncryption
}

func (p *Pipeline) DisableCompression() {
	p.flags &^= EnabledCompression
}

func (p *Pipeline) IsEncryptionEnabled() bool {
	return p.flags&EnabledEncryption != 0
}

func (p *Pipeline) IsCompressionEnabled() bool {
	return p.flags&EnabledCompression != 0
}

func (p *Pipeline) DisableAll() {
	p.flags = 0
}

func (p *Pipeline) SetEncryptor(encryptor Encryptor, secret []byte) error {
	if len(secret) < 16 {
		return errors.New("secret key char length too short")
	}
	p.secret = secret
	p.Encryptor = encryptor
	p.EnableEncryption()
	return nil
}

func (p *Pipeline) SetCompressor(compressor Compressor) {
	p.Compressor = compressor
	p.EnableCompression()
}

func (p *Pipeline) Encode(data []byte) ([]byte, error) {
	var err error
	// 压缩数据
	if p.IsCompressionEnabled() && p.Compressor != nil {
		data, err = p.Compressor.Compress(data)
		if err != nil {
			return nil, fmt.Errorf("failed to compress data: %w", err)
		}

	}

	// 加密数据
	if p.IsEncryptionEnabled() && p.Encryptor != nil {
		data, err = p.Encrypt(p.secret, data)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt data: %w", err)
		}
	}

	return data, nil
}

// fd 必须实现 io.ReadWriteCloser 接口
func (p *Pipeline) Decode(data []byte) ([]byte, error) {
	var err error
	// 解密数据
	if p.IsEncryptionEnabled() && p.Encryptor != nil {
		data, err = p.Decrypt(p.secret, data)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt data: %w", err)
		}
	}

	// 解压缩数据
	if p.IsCompressionEnabled() && p.Compressor != nil {
		data, err = p.Compressor.Decompress(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress data: %w", err)
		}
	}

	return data, nil
}

type Snappy struct{}

func (*Snappy) Compress(data []byte) ([]byte, error) {
	// Snappy 压缩数据
	compressed := snappy.Encode(nil, data)
	return compressed, nil
}

func (*Snappy) Decompress(data []byte) ([]byte, error) {
	// Snappy 解压数据
	return snappy.Decode(nil, data)
}

type Cryptor struct{}

func (*Cryptor) Encrypt(secret, plaintext []byte) ([]byte, error) {
	// Create AES cipher block
	block, err := aes.NewCipher(secret)
	if err != nil {
		return nil, err
	}

	// Padding to block size (AES block size is 16 bytes)
	padding := block.BlockSize() - len(plaintext)%block.BlockSize()
	padText := bytes.Repeat([]byte{byte(padding)}, padding)
	plaintext = append(plaintext, padText...)

	// Create IV
	iv := make([]byte, block.BlockSize())
	_, err = rand.Read(iv)
	if err != nil {
		return nil, err
	}

	// Create cipher using CBC mode
	ciphertext := make([]byte, len(plaintext))
	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext, plaintext)

	// Return IV + ciphertext
	return append(iv, ciphertext...), nil
}

func (*Cryptor) Decrypt(secret, ciphertext []byte) ([]byte, error) {
	// Create AES cipher block
	block, err := aes.NewCipher(secret)
	if err != nil {
		return nil, err
	}

	// Extract IV from the beginning of ciphertext
	iv := ciphertext[:block.BlockSize()]
	ciphertext = ciphertext[block.BlockSize():]

	// Create cipher using CBC mode
	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(ciphertext))
	mode.CryptBlocks(plaintext, ciphertext)

	// Remove padding
	padding := int(plaintext[len(plaintext)-1])
	return plaintext[:len(plaintext)-padding], nil
}
