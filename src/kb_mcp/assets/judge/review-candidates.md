---
prompt_version: judge-review-candidates.v1
---

Review one normalized window payload and return JSON with:
- labels: [{label, score, reasons}]
- should_emit_thin_session: boolean
- carry_forward: boolean
- notes: optional string

Labels:
- adr
- gap
- knowledge

Rules:
- allow multi-label output
- do not emit none together with labels
- keep reasons short and specific

