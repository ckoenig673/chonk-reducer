Synology NAS mounts /tmp with noexec.
Tests that create executable shims must not rely on /tmp.
Use project-local paths instead.