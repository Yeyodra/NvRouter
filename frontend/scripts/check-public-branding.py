#!/usr/bin/env python3
"""Customer-visible NvRouter branding contract; stdlib-only."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

checks = {
    "index.html": ("<title>NvRouter</title>", '/nvrouter-favicon.svg'),
    "src/contexts/BrandingContext.tsx": ('name: "NvRouter"', '/nvrouter-mark.svg', '/nvrouter-favicon.svg'),
    "src/components/AuthGate.tsx": ('"NvRouter"', '/nvrouter-mark.svg'),
    "src/components/Layout.tsx": ('"NvRouter"', 'nvrouter-mark-dark.svg', 'h-11 w-11', 'border-[var(--border)]'),
    "src/components/KimchiConnectModal.tsx": ('Authorize NvRouter in the browser',),
    "src/lib/api.ts": ('name: "NvRouter"',),
    "src/pages/KeyPortal.tsx": ('"NvRouter"',),
    "src/pages/Keys.tsx": ('"NvRouter"',),
    "src/pages/Settings.tsx": ('placeholder="NvRouter"', '/nvrouter-mark.svg'),
    "src/pages/System.tsx": ("NvRouter's own CPU", "NvRouter's resident memory"),
    "src/pages/Guardrails.tsx": ('NvRouter process',),
    "src/components/SavingsCard.tsx": ('NvRouter', '/nvrouter-mark.svg', 'novela.biz.id', 'nvrouter-savings-'),
}

for name, required in checks.items():
    text = (ROOT / name).read_text()
    assert "KeiRouter" not in text, f"customer-visible old brand remains in {name}"
    for value in required:
        assert value in text, f"{value!r} missing from {name}"

for asset in ("public/nvrouter-mark.svg", "public/nvrouter-mark-dark.svg", "public/nvrouter-favicon.svg"):
    text = (ROOT / asset).read_text()
    assert "NvRouter" in text or asset.endswith("favicon.svg"), f"invalid NvRouter asset: {asset}"

print("NvRouter public branding contract: PASS")
