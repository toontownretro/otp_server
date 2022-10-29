import base64, hashlib
from binascii import hexlify

from Crypto.Cipher import DES3
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad

def des3_cbc_encrypt(plaintext, password):
    # Generate 8 random bytes for salting.
    salt = get_random_bytes(8)
    
    # Now we need to derive our IV and key.
    secudata = hashlib.sha256(password+salt).digest()
    key = secudata[:24]
    iv = secudata[24:32]

    # Adjust our key parity.
    key = DES3.adjust_key_parity(key)

    # Create our cipher.
    cipher = DES3.new(key, DES3.MODE_CBC, iv=iv)
    plaintextPadding = pad(plaintext, DES3.block_size)
    ciphertext = cipher.encrypt(plaintextPadding)

    # Make our ciphertext compataible with the OpenSSL executable. (For testing purposes)
    ciphertext = base64.b64encode(b"Salted__" + salt + ciphertext)
    return ciphertext
    
def des3_cbc_decrypt(encryptedData, password):
    # Decode our base64 encoded data.
    encryptedData = base64.b64decode(encryptedData)
    
    # First things first, We want to grab the salt from the ciphertext.
    salt = encryptedData[8:16] # Salt is bytes 8-15 of the ciphertext
    # We don't want the header data anymore.
    encryptedData = encryptedData[16:]
    
    # Now we need to derive our IV and key.
    secudata = hashlib.sha256(password+salt).digest()
    key = secudata[:24]
    iv = secudata[24:32]

    # Adjust our key parity.
    key = DES3.adjust_key_parity(key)
    
    # Create our cipher.
    cipher = DES3.new(key, DES3.MODE_CBC, iv=iv)
    # Decrypt our data.
    plaintext = unpad(cipher.decrypt(encryptedData), DES3.block_size)
    return plaintext