# Tri-On-Edge Project Management

　This document describes how development projects related to Tri-On-Edge are run and managed.

- Ver: 02.e.20260824
- By: IWATA,Y.
- Mod: Added description of the Leader Team, AI-PMO, and AI usage

---

Target Audience
- Project stakeholders

---

Parent Document
- Tri-On-Edge Concept Definition

---

## Working Unit (Team)

- A team is established as the working unit for carrying out development.
  - A team has at most around 5 members.
  - A team has exactly one leader.

- The leader has the following duties and rights:
  - The duty to listen to members' opinions
  - The duty and right to make the team's decisions
  - The duty to communicate the reasons and rationale for decisions to members

- Members have the following duties and rights:
  - The duty and right to hold and express their own opinions
  - As long as the leader fulfills their duties (listening to opinions, making decisions, communicating reasons and rationale), the duty to follow the decision and do their best to contribute their abilities (Disagree and Commit)

## Work Items (Tasks)

- A team's work items are defined as tasks, with the following defined for each. This is to keep the process moving without waiting, rework, or unevenness (Mura).
  - Start condition (the prior process — i.e., the "supplier" — is complete)
  - Completion condition (the next process — i.e., the "customer" — has achieved the required standard)

- A task is owned by one member. Work involving multiple members is split into separate tasks (e.g., creation, review, approval).
  - A member's own task (own process) is considered complete only once it has been handed off to the next process.
  - The leader checks whether a member's workload exceeds one person-effort and takes action if so.

- Each task has a "target effort" and a "target deadline," used as the basis for judging whether action is needed.
  - The maximum target effort is 5 person-days. Beyond that, the task should be split, so that weekly status checks remain easy.
  - The minimum target effort is 0.5 person-days. Below that, tasks should be consolidated, or may be completed without being formally defined as a task, to avoid over-management.

- If a member expects (roughly: an 80% chance) that they will not meet the completion condition within the target effort/deadline, they inform the leader (pull the Andon cord).
  - Members must perform a self-check at the point when either 50% of the effort has been consumed or 50% of the deadline has elapsed, and report the result to the leader.
  - Members distinguish between "facts/phenomena" and "guesses/opinions" when reporting.
  - The leader grasps the facts/phenomena and takes any necessary action directed at the facts/phenomena — not at the individual. Never forget the mindset of "thank you for pulling the Andon cord" and "we gained one more Try & Error experience."

## Leader Team

- For larger projects, a Leader Team is formed, made up of the leaders of each team.
  - Like a regular team, the Leader Team has at most 5 members and one leader (the "senior leader").
  - The roles and tasks of the leader and members follow the same rules as a regular team.

- When forming a Leader Team, the following must first be decided and documented:
  - Mission: what this team exists to achieve
  - Discretion: how much the team's leader is allowed to decide independently
  - The senior leader checks the mission and discretion for gaps, overlaps, and overburden (Muri).
  - The decision-making method follows the same process as within a regular team, and is the senior leader's responsibility.

- If the project is large enough to require it, a further team is formed with senior leaders as its members.
  - This triangular ("Triangle") structure keeps communication costs low and enables JIT decision-making without Muri, Muda, or Mura (Trinity).

## AI-PMO (Leader's Assistant)

- A leader may have generative AI assist with the administrative work involved in task management. This is called AI-PMO.
  - This allows the leader to focus on genuine decision-making.

- When introducing an AI-PMO, the following must first be decided and documented:
  - Mission (the purpose it must fulfill / what it should do)
    - Drafting and maintaining the WBS: based on instructions from the leader, draft tasks (start condition, completion condition, target effort of 0.5–5 person-days, target deadline), and flag any workload exceeding one person-effort or missing task splits for review.
    - Aggregating Andon status and organizing facts: check the presence/format of self-reports at the midpoint checks (50% of effort consumed or 50% of deadline elapsed), and aggregate/visualize only the "facts/phenomena" reported by members.
    - Assisting in maintaining the SSOT (Single Source of Truth) of information: help read, structure, and keep up to date the external documents containing WBS and progress information.

  - Discretion (the authority granted / what it is allowed to do)
    - Access to information: reference and initial organization of the master schedule, achievement scope, member progress, Andon status, etc. needed to build the WBS
    - Format checking and automatic reminders: send fact-checking reminders (formal prompts) for unreported deadlines or format deficiencies

  - Outside the mission/discretion (what it must not do)
    - Final confirmation and evaluation: finalizing the WBS (judging the validity of estimates), or judging the correctness of progress content (determining whether the completion condition has been met)
    - Subjective interpretation/feedback: adding the AI's own "evaluation, speculation, or emotional interpretation" to reports, or giving evaluative feedback to members. This is to prevent reporting from turning into opaque "backroom dealing."

- Points to note
  - State management via external files: WBS and progress information are managed in external documents (shared files, etc.), and loaded into AI-PMO each time as needed.
    - Because AI does not retain memory across sessions.

  - Maintaining direct dialogue (Genchi Genbutsu): the leader does not rely solely on AI-PMO's reports, and continues to talk directly with members as needed.
    - This is to prevent AI from becoming the sole information channel and causing a disconnect from reality.

## AI Usage (Member's Assistant)

- A member may have generative AI assist with the execution of their own tasks.
  - Feed the development policy, coding rules, etc. into the generative AI to reduce variance/unevenness (Mura) in output (quality) (managing the SSOT/Single Source of Truth of information; making work standards explicit).

## Development / Evaluation Process

### Before Coding

- Create a release note (draft) and have it reviewed.
  - The format (file, Git comment, etc.) is determined by the leader.
  - The file name and commit message must include the "issue date" and "sender (By:)."
  - Why: describe the purpose of the change (why it's being changed, what it aims to achieve) in about 1–2 sentences.
  - To Check: list the evaluation criteria (how the achievement of the purpose will be confirmed) as bullet points.
  - Module: describe the expected target module(s) (file name) of the change and the starting version (Version(from):).

Example release note (draft)
```text
2025.12.11    By: IWATA,Y.

Why: Eliminate the increase in memory/storage usage during continuous operation

To Check:
- Does memory usage stay flat during continuous operation?
- Does storage usage stay flat during continuous operation, outside of designated locations such as log files?

Module: 10_stream_core
Version(from): 01.a.20251210

Module: 19_util
Version(from): 01.a.20251210
...
```

#### During Coding

- Refining the evaluation items
  - Once implementation details become clear, add concrete confirmation items — based on the evaluation criteria — to the release note (draft).

- Revising the pre-coding assumptions
  - Actively revise the target modules and evaluation criteria in the release note (draft) based on implementation details.
  - However, check that this does not deviate from the original purpose. If the purpose itself needs to change, get it reviewed again.

#### After Coding

- Based on the release note (draft), create the final release note and have it reviewed.
  - The format (file, Git comment, etc.) is determined by the leader.
  - The file name and commit message must include the "issue date" and "sender."
  - Based on the actual coding performed, describe:
    - Module: the name(s) (file name) of the module(s) changed
    - Version(to): the version after the change
    - Modified: the function name(s) and other specifics of what was changed
  - If the change involves an environment change, such as adding an external library, describe it under the change content.
  - Checked: describe the evaluation items (based on the evaluation criteria, plus any standard baseline checks) and the results of self-verification (debugging).

Example release note
```text
2025.12.14    By: IWATA,Y.    (Approved:  2025.12.15  By: IWATA,Z.)

Why: Eliminate the increase in memory/storage usage during continuous operation

To Check:
- Does memory usage stay flat during continuous operation?
- Does storage usage stay flat during continuous operation, outside of designated locations such as log files?

Checked:
- Does memory usage stay flat during continuous operation?
  - (Describe the evaluation item refined during implementation, and the self-verification result.)
  - ...

- Does storage usage stay flat during continuous operation, outside of designated locations such as log files?
  - (Describe the evaluation item refined during implementation, and the self-verification result.)
  - ...

- Does existing functionality continue to work unchanged (basic operation check)?
  - (If a standard evaluation item exists, describe it along with the self-verification result.)
  - ...

Module: 10_stream_core
Version(from): 01.a.20251210
Version(to)  : 01.b.20251214
Modified: Changed the judgment method inside func()

Module: 19_util
Version(from): 01.a.20251210
Version(to)  : 02.a.20251214
Modified: Newly added func_add()
...
```

#### During Review

- The reviewee must not rely solely on verbal explanation to convey content that is not expressed in the code or documentation, since someone looking at it years later will not be able to hear that explanation.
  - This is not meant to ban verbal explanation or communication entirely — the goal is for the necessary content to be expressed in the code/documentation (so that it survives into the future).
  - It is fine to reflect it after the review, but any necessary content must always be incorporated into the code/documentation.

- When making a comment, the reviewer must responsibly communicate the reason, background, and rationale for it to the reviewee. Project stakeholders must not act on a comment that lacks this, since it risks creating Muri, Muda, or Mura.

- Actively conduct written reviews, to avoid wasted waiting time through asynchronous communication, and because it makes recording and accumulating review content easy.
  - Asynchronous communication should generally follow a single round-trip pattern (send → comment back → sender confirms).
  - If a single round-trip is not sufficient, switch to synchronous communication (a live conversation).

End of document
