package credential

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
)

type Signer interface {
	Sign(text string) string
}

type Credential interface {
	Signer
	KeyId() string
	KeySecret() string
}

type SimpleCredential struct {
	accessKeyId     string
	accessKeySecret string
}

func New(accessKeyId, accessKeySecret string) Credential {
	return &SimpleCredential{accessKeyId: accessKeyId, accessKeySecret: accessKeySecret}
}

func (c *SimpleCredential) KeyId() string {
	return c.accessKeyId
}

func (c *SimpleCredential) KeySecret() string {
	return c.accessKeySecret
}

func (c *SimpleCredential) Sign(text string) string {
	return ShaHmac1(text, c.accessKeySecret)
}

func ShaHmac1(source, secret string) string {
	hash := hmac.New(sha1.New, []byte(secret))
	hash.Write([]byte(source))
	return base64.StdEncoding.EncodeToString(hash.Sum(nil))
}
