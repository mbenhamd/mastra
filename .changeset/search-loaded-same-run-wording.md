---
'@mastra/core': patch
---

With `autoLoad`, ToolSearchProcessor and SkillSearchProcessor no longer tell the model that a loaded tool or skill becomes available "on your next turn". A loaded tool is callable on the next step of the same run, and loaded skill instructions are added on that step. The system text, the `search_tools` and `search_skills` descriptions, and the search and `load_tool` result messages now say so.
