package service

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
)

const (
	pbkdf2Iterations = 600_000
	saltSize         = 32
	keySize          = 32
	nonceSize        = 12 // AES-GCM standard nonce size
)

// encryptData encrypts data with AES-256-GCM using a password-derived key.
// Output format: salt(32B) || nonce(12B) || ciphertext+GCM-tag
func encryptData(data []byte, password string) ([]byte, error) {
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("generate salt: %w", err)
	}

	key, err := pbkdf2.Key(sha256.New, password, salt, pbkdf2Iterations, keySize)
	if err != nil {
		return nil, fmt.Errorf("derive key: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nil, nonce, data, nil)

	// salt || nonce || ciphertext+tag
	result := make([]byte, 0, saltSize+nonceSize+len(ciphertext))
	result = append(result, salt...)
	result = append(result, nonce...)
	result = append(result, ciphertext...)

	return result, nil
}

// DecryptData decrypts AES-256-GCM encrypted data using a password-derived key.
// Input format: salt(32B) || nonce(12B) || ciphertext+GCM-tag
func DecryptData(encrypted []byte, password string) ([]byte, error) {
	minLen := saltSize + nonceSize + 1
	if len(encrypted) < minLen {
		return nil, fmt.Errorf("encrypted data too short")
	}

	salt := encrypted[:saltSize]
	nonce := encrypted[saltSize : saltSize+nonceSize]
	ciphertext := encrypted[saltSize+nonceSize:]

	key, err := pbkdf2.Key(sha256.New, password, salt, pbkdf2Iterations, keySize)
	if err != nil {
		return nil, fmt.Errorf("derive key: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed (wrong password or corrupted data): %w", err)
	}

	return plaintext, nil
}
