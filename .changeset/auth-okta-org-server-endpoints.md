---
'@mastra/auth-okta': patch
---

Fixed Okta org authorization server support. Previously, authorization requests to Okta org servers failed with 404 errors because endpoints were constructed incorrectly. Authorization, token, keys, and logout endpoints now resolve correctly for both custom and org authorization servers.
