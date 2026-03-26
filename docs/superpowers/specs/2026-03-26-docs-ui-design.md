# Genie World Documentation UI Design

**Date:** 2026-03-26
**Status:** Approved

## Overview

Redesign the Docusaurus documentation site for Genie World with a Databricks-branded, clean & light aesthetic. The site serves both internal SAs and external customers.

## Color Palette

| Token | Hex | Usage |
|-------|-----|-------|
| Oatmeal | `#F5F2EE` | Page background, navbar background (`--ifm-navbar-background-color`) |
| White | `#FFFFFF` | Content cards, hero panel, doc content area |
| Navy | `#1B3A4B` | Headings, body text, primary buttons, footer background |
| Warm gray | `#8C877D` | Secondary/muted text, table headers (uppercase) |
| Border | `#E5DDD4` | Card borders, dividers, h2 underlines |
| Red | `#FF3621` | Step numbers, active sidebar links, logo mark, inline link hover. Sparse — never dominant. |
| Code bg | `#F6F8FA` | Code block background (GitHub-light style) |
| Code border | `#E5E7EB` | Code block border |

### Dark Mode

Respect `prefers-color-scheme`. Token mapping:

| Light token | Dark value | Notes |
|-------------|-----------|-------|
| Oatmeal `#F5F2EE` | `#1A1816` | Page background |
| White `#FFFFFF` | `#242220` | Content surfaces |
| Navy `#1B3A4B` | `#E8E3DC` | Inverted for text on dark backgrounds |
| Warm gray `#8C877D` | `#9E9890` | Slightly lighter for readability |
| Border `#E5DDD4` | `#3A3633` | Muted warm dark border |
| Red `#FF3621` | `#FF5240` | Slightly lighter for contrast on dark |
| Code bg `#F6F8FA` | Dracula theme | Use Prism dracula theme |
| Footer `#1B3A4B` | `#141210` | Darker than page bg |

## Typography

- **Body:** Inter, -apple-system, sans-serif fallback
- **Code:** JetBrains Mono, Fira Code fallback
- **Heading weight:** 600 (semibold)
- **h1:** 2rem, letter-spacing -0.03em
- **h2:** 1.4rem, letter-spacing -0.02em, bottom border in `#E5DDD4`
- **Body line-height:** 1.7
- **Code font-size:** 90% of body

## Landing Page

Minimal layout:

1. **Navbar** — oatmeal background (`#F5F2EE`), red logo square + "Genie World" in navy, nav links right-aligned
2. **Hero** — full-width oatmeal canvas. Centered white card panel (`max-width: 720px`, `padding: 3rem`, `border-radius: 12px`, no box-shadow, `border: 1px solid #E5DDD4`). Title in navy, subtitle in warm gray, two buttons:
   - "Get Started" — navy (`#1B3A4B`) fill, white text
   - "User Guide" — white fill, navy text, `1px solid #E5DDD4` border
3. **Feature cards** — 3 white cards on oatmeal. Each has a red step number rendered as plain text in format "01" / "02" / "03" (not a filled circle), navy title, warm gray description. Subtle hover: border shifts from `#E5DDD4` to `#D4CCC2`.
4. **Footer** — navy (`#1B3A4B`) background, white/muted text, minimal links (Docs, GitHub)

No blog. No extra sections.

## Docs Pages

- **Page background:** Oatmeal — set `--ifm-background-color: #F5F2EE`
- **Content area:** White panel with comfortable padding
- **Sidebar:** On oatmeal background. Active link gets red text + subtle `rgba(255, 54, 33, 0.06)` background
- **Code blocks:** GitHub-light syntax highlighting, `#F6F8FA` background, `#E5E7EB` border, 6px border-radius
- **Tables:** 0.9rem font, uppercase headers with hardcoded `table th { color: #8C877D; }` (not a CSS variable — Infima has no warm-gray token), `#E5DDD4` borders
- **TOC (right sidebar):** Smaller font (0.825rem), active link in red
- **Breadcrumbs:** Default Docusaurus, no custom styling needed

## Components Changed

| File | What changes |
|------|-------------|
| `website/src/css/custom.css` | Full rewrite — all Infima variable overrides (including `--ifm-navbar-background-color: #F5F2EE`), Databricks palette, typography, code blocks, sidebar active states, table th color override to warm gray, footer styling, full dark mode token map |
| `website/src/pages/index.module.css` | Full rewrite (discard existing dark-gradient hero, filled-circle step badge, and white-text secondary button styles entirely) — oatmeal canvas, white hero card with max-width/border/radius, feature cards with hover, navy primary button, navy-text outline secondary button |
| `website/src/pages/index.js` | Full rewrite (discard existing dark hero and circle badge markup) — hero with centered white card, "01"/"02"/"03" plain-text step numbers, navy primary button, outline secondary button with navy text |

Note: `website/docusaurus.config.js` already has correct Prism themes (github/dracula) and `additionalLanguages: ['bash', 'python']` — no changes needed.

## Out of Scope

- Custom logo/icon design
- Search configuration
- Blog
- Versioning
- Content changes to docs markdown files
