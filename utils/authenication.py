import hashlib

def make_hashes(password: str) -> str:
    """Convert password to a secure hash."""
    return hashlib.sha256(str(password).encode('utf-8')).hexdigest()

def check_hashes(password: str, hashed_text: str) -> bool:
    """Check if the provided password matches the stored hash."""
    if make_hashes(password) == hashed_text:
        return True
    return False