# Git Hooks for Binary File Prevention

This repository includes a pre-commit hook that prevents binary files from being accidentally committed to the repository.

## How it works

The pre-commit hook runs automatically before each commit and checks all staged files for binary content based on the actual file content, not just file extensions. The hook uses multiple methods to detect binary files:

1. File type detection using the `file` command
2. Null byte detection (binary files often contain null bytes)
3. Non-printable character ratio analysis

The hook prioritizes text files by first checking if the `file` command identifies them as text, ASCII, UTF-8, Unicode, source code, or scripts.

## Installation

The hook is automatically installed in the `.git/hooks/` directory. If you need to reinstall it, simply copy the `pre-commit` file to `.git/hooks/pre-commit` and make it executable:

```bash
chmod +x .git/hooks/pre-commit
```

## Configuration

The hook can be customized by modifying the `.git/hooks/pre-commit` file. You can adjust the thresholds for binary file detection or add additional file type checks.

## Bypassing the hook

In rare cases where you need to commit a file that the hook incorrectly identifies as binary, you can bypass the hook using:

```bash
git commit --no-verify -m "Your commit message"
```

However, this should only be done after careful consideration and approval from the repository maintainers.

## Adding files to .gitignore

Instead of committing binary files, add them to the `.gitignore` file:

```bash
echo "filename.ext" >> .gitignore
```

Additionally, all files without extensions are now ignored by default in the `.gitignore` file. If you need to commit a specific file without an extension, you can explicitly include it:

```bash
echo "!filename" >> .gitignore
```

## Recent Improvements

We've recently enhanced our binary file prevention system:

1. **Content-based detection**: The pre-commit hook now detects binary files based on their actual content rather than just file extensions
2. Added more comprehensive rules to `.gitignore` to exclude profiling results and build artifacts
3. Improved the pre-commit hook to be more accurate in detecting binary files while avoiding false positives
4. Removed all existing binary files from the repository history
5. Added specific exclusions for profiling directories and files

These changes ensure that binary files will not accidentally be committed to the repository in the future, while still allowing legitimate text files to be committed regardless of their extensions.