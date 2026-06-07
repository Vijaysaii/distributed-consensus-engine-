# crypto_utils.py
# this file handles key generation and message signing for PBFT
# written by Vijay Sai Krishna Devabhakthuni - G25AI1016

from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization
import json


def generate_keys():
    # generate a new RSA key pair for this node
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    public_key = private_key.public_key()
    return private_key, public_key


def sign_message(private_key, message: dict) -> bytes:
    # sign a dictionary message using RSA-PSS
    # we sort the keys so the signature is consistent
    msg_bytes = json.dumps(message, sort_keys=True).encode()
    signature = private_key.sign(
        msg_bytes,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    return signature


def verify_message(public_key, message: dict, signature: bytes) -> bool:
    # check if the signature on a message is valid
    # returns True if valid, False if tampered or forged
    try:
        msg_bytes = json.dumps(message, sort_keys=True).encode()
        public_key.verify(
            signature,
            msg_bytes,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return True
    except Exception:
        return False


def serialize_public_key(public_key) -> str:
    # convert public key to string so we can send it over network
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return pem.decode()


def deserialize_public_key(pem_str: str):
    # convert string back to public key object
    return serialization.load_pem_public_key(pem_str.encode())
