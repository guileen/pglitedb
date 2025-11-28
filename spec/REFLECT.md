# Context Usage Reflection Guidelines

## Purpose
This document provides guidelines for reflecting on context usage to enable continuous improvement of the PGliteDB documentation system.

## Required Reflection Elements

### 1. File Contribution Assessment (1-10 rating)
Rate each file's contribution to solving your specific problem:
- 1-3: Minimal help, peripheral to task
- 4-6: Somewhat helpful, provided some guidance
- 7-8: Helpful, contributed meaningfully to solution
- 9-10: Essential, critical to task completion

### 2. Spec Document Effectiveness (1-10 rating)
Rate the effectiveness of each spec document:
- 1-3: Poor organization, hard to navigate, unclear information
- 4-6: Adequate but could be improved
- 7-8: Good structure and content, generally helpful
- 9-10: Excellent organization, clear content, highly effective

### 3. Key Lessons Learned
Document specific insights gained during task execution:
- What worked well with the context system?
- What challenges did you encounter?
- What information was missing or unclear?
- What debugging approaches proved most effective?

### 4. Improvement Suggestions
Provide specific suggestions for enhancing future task execution:
- What changes would make the context more effective?
- What additional information would be helpful?
- How could the organization be improved?
- What patterns or templates would be useful?

## Example Reflection Format

```
## Task: [Brief task description]

### 1. File Contribution Assessment
1. spec/Context_Catalog.md - 9/10
   - Provided critical information about system table relationships
   - Clear documentation of OID consistency requirements
2. spec/Context_DDL.md - 8/10
   - Helped understand metadata persistence issues
   - Good cross-referencing with catalog context

### 2. Spec Document Effectiveness
1. spec/Context.md - 10/10
   - Excellent central navigation hub
   - Clear prioritization of issues
2. spec/Context_Catalog.md - 9/10
   - Well-structured with good examples
   - Effective use of weight markers

### 3. Key Lessons Learned
- System table relationships are fundamental to database integrity
- OID consistency is critical for metadata persistence
- Cross-component debugging requires understanding multiple context files

### 4. Improvement Suggestions
- Add more concrete debugging examples for common issues
- Include more specific file paths and line numbers
- Create templates for common problem-solving patterns
```

## Submission Requirements
All context users must submit reflections following this format to ensure continuous improvement of the documentation system.