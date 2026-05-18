---
'@mastra/server': patch
---

Improved Harness route authentication security. Harness routes now require configured authentication, reject access tokens passed in URL query parameters, and keep event subscription tokens out of shared request state. Move clients that send access tokens in query parameters to `Authorization` headers.
