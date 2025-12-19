import os

from cryptography.fernet import Fernet


class Encrypter:
    def __init__(self):
        self.cipher = Fernet(os.getenv("DB_ENCRYPT_KEY"))

    def encrypt(self, data: str) -> str:
        """Encrypt a string and return a string (base64 encoded)."""
        encrypted_bytes = self.cipher.encrypt(data.encode())
        return encrypted_bytes.decode("utf-8")

    def decrypt(self, data: str) -> str:
        """Decrypt a string (base64 encoded) and return the original string."""
        encrypted_bytes = data.encode("utf-8")
        decrypted_bytes = self.cipher.decrypt(encrypted_bytes)
        return decrypted_bytes.decode("utf-8")
