#!/usr/bin/env python3
from pathlib import Path

source = (Path(__file__).parents[1] / "src/pages/KeyPortal.tsx").read_text()
login = source[source.index("const handleLogin"):source.index("const handleLogout")]

assert 'val.startsWith("kr_")' in login, "NvRouter kr_ API keys must use key=, not id="
assert 'val.startsWith("sk-")' in login, "legacy sk- API keys must remain supported"

assert "if (activeKey && data?.key_id)" in source, "verified API keys must redirect to their portal ID"
assert "setParams({ id: data.key_id }, { replace: true })" in source, "API key must be removed from browser history after verification"
print("Portal identifier routing check passed")
