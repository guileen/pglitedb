# Multilingual Documentation Structure

This directory contains documentation for PGLiteDB in multiple languages.

## Available Languages

- [English](../README.md) - Main documentation in English
- [中文](./zh/README.md) - Chinese documentation
- [Español](./es/README.md) - Spanish documentation
- [日本語](./ja/README.md) - Japanese documentation

## Documentation Organization

Each language directory follows the same structure:

```
docs/
├── README.md (Main English documentation)
├── NAVIGATION.md (Navigation guide)
├── api/
│   └── reference.md (API reference)
├── guides/
│   ├── quickstart.md (Quick start guides)
│   ├── embedded_usage.md (Embedded usage guide)
│   └── interactive_examples.md (Interactive examples)
└── [language]/
    ├── README.md (Language-specific introduction)
    ├── api/
    │   └── reference.md (Translated API reference)
    ├── guides/
    │   ├── quickstart.md (Translated quick start guides)
    │   ├── embedded_usage.md (Translated embedded usage guide)
    │   └── interactive_examples.md (Translated interactive examples)
    └── ...
```

## Translation Status

| Language | Documentation Coverage | Translation Progress |
|----------|------------------------|----------------------|
| English  | 100%                   | 100%                 |
| Chinese  | Introduction + Performance Optimizations | 10%              |
| Spanish  | Introduction Only      | 5%                   |
| Japanese | Introduction Only      | 5%                   |

## Contributing Translations

We welcome translations from the community! To contribute:

1. Fork the repository
2. Create a branch for your translation work
3. Translate documents following the existing structure
4. Submit a pull request

Please ensure translations maintain technical accuracy while being culturally appropriate for the target audience.