from __future__ import annotations

import os
import subprocess
from typing import Optional


SECRET_ENV_VAR = "CHONK_SECRET_KEY"
SECRET_PREFIX = "enc::"


class SecretConfigError(RuntimeError):
    pass


def _secret_key() -> Optional[str]:
    value = str(os.getenv(SECRET_ENV_VAR, "") or "").strip()
    return value or None


def is_encrypted(value: str) -> bool:
    return str(value or "").startswith(SECRET_PREFIX)


def _openssl_crypt(value: str, decrypt: bool) -> str:
    key = _secret_key()
    if not key:
        action = "read encrypted" if decrypt else "save"
        raise SecretConfigError("%s is required to %s secret settings." % (SECRET_ENV_VAR, action))

    cmd = ["openssl", "enc", "-aes-256-cbc", "-pbkdf2", "-a", "-A", "-pass", "pass:%s" % key]
    if decrypt:
        cmd.insert(3, "-d")

    proc = subprocess.run(
        cmd,
        input=value.encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        raise SecretConfigError("Encrypted secret settings could not be decrypted. Re-enter the secret values.")
    return proc.stdout.decode("utf-8")


def encrypt_secret(value: str) -> str:
    plain = str(value or "")
    if not plain:
        return ""
    token = _openssl_crypt(plain, decrypt=False)
    return SECRET_PREFIX + token


def decrypt_secret(value: str) -> str:
    stored = str(value or "")
    if not stored:
        return ""
    if not is_encrypted(stored):
        return stored
    return _openssl_crypt(stored[len(SECRET_PREFIX) :], decrypt=True)
