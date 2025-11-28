# Git Hooks for Binary File Prevention

This repository includes a pre-commit hook that prevents binary files from being accidentally committed to the repository.

## How it works

The pre-commit hook runs automatically before each commit and checks all staged files for:

1. Files with extensions commonly associated with binary files (e.g., .exe, .dll, .so, .dylib, .app, .jar, .war, .zip, .tar, .gz, etc.)
2. Files detected as binary by the `file` command
3. Files containing a high percentage of non-printable characters

Note: Files without extensions are now excluded from binary checking as per user requirements.

## Installation

The hook is automatically installed in the `.git/hooks/` directory. If you need to reinstall it, simply copy the `pre-commit` file to `.git/hooks/pre-commit` and make it executable:

```bash
chmod +x .git/hooks/pre-commit
```

## Configuration

The hook can be customized by modifying the `.git/hooks/pre-commit` file. You can add or remove file extensions from the `forbidden_extensions` regex pattern.

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

1. Added more comprehensive rules to `.gitignore` to exclude profiling results and build artifacts
2. Improved the pre-commit hook to be more accurate in detecting binary files
3. Removed all existing binary files from the repository history
4. Added specific exclusions for profiling directories and files

These changes ensure that binary files will not accidentally be committed to the repository in the future.