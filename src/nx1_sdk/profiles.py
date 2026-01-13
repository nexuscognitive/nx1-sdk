"""Profile management for NX1 SDK.

Profiles allow storing multiple API configurations in ~/.nx1/profiles (YAML format).

Example ~/.nx1/profiles:
```yaml
default:
  host: https://api.production.nx1cloud.com
  api_key: psk_prod_xxxxx
  verify_ssl: true

dev:
  host: https://api.dev.nx1cloud.com
  api_key: psk_dev_xxxxx
  verify_ssl: false

staging:
  host: https://api.staging.nx1cloud.com
  api_key: psk_staging_xxxxx
```

Usage:
    # Use default profile
    client = NX1Client(profile="default")
    
    # Use named profile
    client = NX1Client(profile="dev")
    
    # CLI
    nx1 --profile dev domains
    nx1 --profile staging jobs list
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from nx1_sdk.exceptions import NX1ValidationError


# Default profile directory and file
DEFAULT_PROFILE_DIR = Path.home() / ".nx1"
DEFAULT_PROFILE_FILE = DEFAULT_PROFILE_DIR / "profiles"


def get_profile_path() -> Path:
    """Get the path to the profiles file."""
    # Allow override via environment variable
    env_path = os.environ.get("NX1_PROFILES_PATH")
    if env_path:
        return Path(env_path)
    return DEFAULT_PROFILE_FILE


def load_profiles() -> Dict[str, Dict[str, Any]]:
    """
    Load all profiles from the profiles file.
    
    Returns:
        Dictionary of profile name -> profile config
    """
    profile_path = get_profile_path()
    
    if not profile_path.exists():
        return {}
    
    try:
        with open(profile_path, 'r') as f:
            content = f.read()
            if not content.strip():
                return {}
            profiles = yaml.safe_load(content)
            return profiles if profiles else {}
    except yaml.YAMLError as e:
        raise NX1ValidationError(f"Invalid YAML in profiles file {profile_path}: {e}")
    except Exception as e:
        raise NX1ValidationError(f"Error reading profiles file {profile_path}: {e}")


def get_profile(profile_name: str) -> Dict[str, Any]:
    """
    Get a specific profile by name.
    
    Args:
        profile_name: Name of the profile to load
    
    Returns:
        Profile configuration dictionary
    
    Raises:
        NX1ValidationError: If profile not found
    """
    profiles = load_profiles()
    
    if profile_name not in profiles:
        available = list(profiles.keys()) if profiles else []
        raise NX1ValidationError(
            f"Profile '{profile_name}' not found. "
            f"Available profiles: {available if available else 'none (create ~/.nx1/profiles)'}"
        )
    
    return profiles[profile_name]


def list_profiles() -> Dict[str, Dict[str, Any]]:
    """
    List all available profiles.
    
    Returns:
        Dictionary of all profiles
    """
    return load_profiles()


def save_profile(
    profile_name: str,
    host: str,
    api_key: str,
    verify_ssl: bool = True,
    timeout: int = 30
) -> None:
    """
    Save a profile to the profiles file.
    
    Args:
        profile_name: Name for the profile
        host: API host URL
        api_key: API key (PSK)
        verify_ssl: Whether to verify SSL certificates
        timeout: Request timeout in seconds
    """
    profile_path = get_profile_path()
    
    # Ensure directory exists
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load existing profiles
    profiles = load_profiles()
    
    # Add/update profile
    profiles[profile_name] = {
        "host": host,
        "api_key": api_key,
        "verify_ssl": verify_ssl,
        "timeout": timeout
    }
    
    # Save profiles
    with open(profile_path, 'w') as f:
        yaml.dump(profiles, f, default_flow_style=False, sort_keys=False)
    
    # Set restrictive permissions (owner read/write only)
    os.chmod(profile_path, 0o600)


def delete_profile(profile_name: str) -> bool:
    """
    Delete a profile from the profiles file.
    
    Args:
        profile_name: Name of the profile to delete
    
    Returns:
        True if deleted, False if not found
    """
    profiles = load_profiles()
    
    if profile_name not in profiles:
        return False
    
    del profiles[profile_name]
    
    profile_path = get_profile_path()
    with open(profile_path, 'w') as f:
        yaml.dump(profiles, f, default_flow_style=False, sort_keys=False)
    
    return True


def resolve_config(
    api_key: Optional[str] = None,
    host: Optional[str] = None,
    profile: Optional[str] = None,
    verify_ssl: Optional[bool] = None,
    timeout: Optional[int] = None
) -> Dict[str, Any]:
    """
    Resolve configuration from CLI args, environment variables, and profiles.
    
    Priority (highest to lowest):
    1. Explicit CLI arguments (api_key, host, etc.)
    2. Environment variables (NX1_API_KEY, NX1_HOST)
    3. Profile configuration (if profile specified or 'default' exists)
    
    Args:
        api_key: Explicit API key
        host: Explicit host URL
        profile: Profile name to load
        verify_ssl: Explicit SSL verification setting
        timeout: Explicit timeout setting
    
    Returns:
        Resolved configuration dictionary with keys:
        - api_key
        - host
        - verify_ssl
        - timeout
    """
    config: Dict[str, Any] = {
        "api_key": None,
        "host": None,
        "verify_ssl": True,
        "timeout": 30
    }
    
    # Step 1: Load profile if specified, or try 'default' profile
    profile_config: Dict[str, Any] = {}
    if profile:
        # Explicit profile requested - must exist
        profile_config = get_profile(profile)
    else:
        # Try to load 'default' profile silently
        try:
            profiles = load_profiles()
            if "default" in profiles:
                profile_config = profiles["default"]
        except Exception:
            pass  # Ignore errors when no explicit profile requested
    
    # Apply profile config as base
    if profile_config:
        config["api_key"] = profile_config.get("api_key")
        config["host"] = profile_config.get("host")
        config["verify_ssl"] = profile_config.get("verify_ssl", True)
        config["timeout"] = profile_config.get("timeout", 30)
    
    # Step 2: Override with environment variables
    env_api_key = os.environ.get("NX1_API_KEY") or os.environ.get("LAKEHOUSE_API_KEY")
    env_host = os.environ.get("NX1_HOST") or os.environ.get("LAKEHOUSE_HOST")
    
    if env_api_key:
        config["api_key"] = env_api_key
    if env_host:
        config["host"] = env_host
    
    # Step 3: Override with explicit arguments (highest priority)
    if api_key:
        config["api_key"] = api_key
    if host:
        config["host"] = host
    if verify_ssl is not None:
        config["verify_ssl"] = verify_ssl
    if timeout is not None:
        config["timeout"] = timeout
    
    return config
