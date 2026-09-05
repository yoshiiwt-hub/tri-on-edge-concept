# Tri-On-Edge Development Concept

　This document presents the guiding principles for implementing Tri-On-Edge software.

- Ver: 02.d.20260824
- By: IWATA,Y.
- Mod: Initial creation

---

Target Audience
- Developers: all chapters

---

Parent Document
- Tri-On-Edge Concept Definition

---

## 1. Basic Principles

### 1. Readability for Your Future Self and Teammates (Trinity)

- Write code and documentation that is readable and maintainable by people other than "your current self."
- Write sentences with a clear subject, object, and predicate, in short form. Avoid overusing conjunctions or otherwise building complex sentence structures.
  - This is to minimize ambiguity and room for interpretation.
  - (Unless otherwise stated, the implied subject of sentences in this policy is "the person writing the program.")
- For anything not covered here, follow Python's standard PEP 8 style guide.

### 2. Continuous Improvement (Trial)

- This policy itself should be grown through repeated practical application, evaluation, and improvement.
- This policy should describe not only rules (what to do) but also the reasoning and rationale (why to do it).
  - This is so the underlying thinking can keep improving, rather than being followed thoughtlessly just because "it's the rule."
- This policy is a set of principles, not absolutes; deviation is acceptable if a valid reason is recorded and explained. Reflect that reasoning back into future improvements.
  - (There is no need to add "in principle" to each statement — it is implied throughout.)

### 3. Applying the Manufacturing-Floor / TPS Mindset

- Genchi Genbutsu (現地・現物 — go and see the actual place and the actual thing)
  - Prioritize the actual implementation, actual data, and actual phenomena (facts).

- Naze Naze (なぜなぜ — ask "why" repeatedly)
  - Pursue not only "what" but "why" — the underlying principles and reasons.

- Jidoka (自働化 — automation with a human touch), "don't make people watchdogs for machines"
  - The system should detect its own state and, on abnormality, stop and notify (Supervise).

- Just-In-Time, "the next process is your customer"
  - Complete your own process by the timing (deadline) the next process requires.
  - Complete your own process to the content (quality standard) the next process requires.

- Eliminate Muri, Mura, and Muda (overburden, unevenness, and waste)
  - Eliminate Muri (overburden)
    - (Example) Backpressure control: when input is excessive, the system throttles via queue limits to prevent a system-down condition (Muri).
  - Eliminate Mura (unevenness)
    - (Example) Asynchronous processing: heavy processing is offloaded to a separate thread, leveling out the processing time of the main loop.
  - Eliminate Muda (waste)
    - (Example) Filtering: static data with no change is not saved or transmitted.

- 4S (Seiri, Seiton, Seisou, Seiketsu — sort, set in order, shine, standardize) and the 5 Fixed Points (fixed route, fixed quantity, fixed location, fixed name, fixed color)
  - Seiri (sort): discard unnecessary data and code.
  - Seiton (set in order): keep necessary data and code readily retrievable.
  - Seisou (shine): inspect for and remove unnecessary data and code.
  - Seiketsu (standardize): maintain the clean state achieved above.

- Work Standards, Standardized Work
  - Document the work content and quality standards explicitly.

### 4. Leveraging the Existing Environment (Retrofit)

　The basic approach is retrofitting: minimizing modification or downtime of existing equipment, systems, and software, and instead adding data connectivity and functionality from the outside.

　Manufacturing sites have many pieces of equipment and control systems that have been running for years. Replacing all of them is not realistic in terms of cost, risk, and downtime.

　Tri-On-Edge makes use of existing equipment and environments and connects data from the outside (Trinity), enabling a small, fast start to digital transformation (DX).

### Keywords

- **Trigger** — The execution unit of processing; generally equivalent to a "Worker."
- **Find Trigger** — A Trigger that performs input-direction processing toward the Core, such as reading PLC data or detecting files.
- **Act Trigger** — A Trigger that performs output-direction processing from the Core, such as writing PLC data or lifting data to the cloud.
- **Segment (Trigger Segment)** — A meaningful, contiguous block of data: a waveform, an interval, or continuous cycle data.
- **Point** — A single value belonging to one column, one row, and one key. The smallest unit of data.
- **Supervise** — The domain of monitoring, management, and operations.
- **Stream** — The domain of real-time data.
- **Stored** — The domain of stored/accumulated data.
- **Simulate** — The domain of virtual verification.

See: [Glossary](#glossary)

### System Domain Numbering

　The "system domains" refer to Supervise, Stream, Stored, and Simulate. System domain numbering is as follows:

- 00-series: Supervise (management, monitoring, common infrastructure)
- 10-series: Stream (real-time)
- 20-series: Stored (accumulation, analysis)
- 30-series: Simulate (virtual verification)

## Closing

　This policy is not meant to constrain behavior, but to serve as shared understanding for advancing "Try on the Edge Now, KAIZEN the Future." efficiently, without rework or stagnation.

End of document

---

## Glossary

| Concept | Common Name (when specifically needed) | Term in Code (formatting follows naming conventions) | Abbreviation (only when context is clear) | Description / Notes |
|:--------|:-----------------------------------------|:-------------------------------------------------------|:---------------------------------------------|:----------------------|
| Trigger | Trigger | trigger | trg | — |
| Find Trigger | Find Trigger | find_trigger | f_trigger<br>f | — |
| Act Trigger | Act Trigger | act_trigger | a_trigger<br>a | — |
| Setting | Setting<br>Set | setting | set | Do not use "param," "config," etc. |
| Threshold | Border | border | — | Generally called "threshold" elsewhere, but we deliberately use "border" for brevity and to keep the project's terminology consistent. |
| A meaningful, contiguous block of data — a waveform, interval, or continuous cycle data | Waveform<br>Segment | trigger_segment | segment<br>seg | Data from Trigger-On to Trigger-Off. |
| The state of a waveform | Status | trigger_status | status | — |
| Transition of a waveform into its start state | Trigger-On | trigger_on | on | — |
| Transition of a waveform into its end state | Trigger-Off | trigger_off | off | — |
| A single value | Point | trigger_point | point | — |
| The first point of a waveform | Start Point | trigger_on_point | on_point | — |
| A point within the body of a waveform | Mid Point | trigger_mid_point | mid_point | — |
| The last point of a waveform | End Point | trigger_off_point | off_point | — |
| A point that is not part of a waveform | Non-Waveform Point | non_trigger_point | non_point | — |
| A column, field, variable, dimension of waveform data | Column | column | col | Acceptable as a standalone term for things like an objective variable. |
| A row or record of waveform data | Row<br>Record | record | rec | — |
