---
'@mastra/core': patch
---

Fixed `SkillSearchProcessor` to count distinct skills toward `topK`, so matching reference files do not crowd out another eligible skill. Document results remain the default for Workspace skill search. To request distinct skills directly:

```typescript
const results = await workspace.skills.search('code review', {
  topK: 3,
  distinctSkills: true,
});
```
