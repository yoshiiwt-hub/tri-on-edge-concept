# Five Questions for Data Utilization

　A framework for framing problems from the manufacturing site, used at the very first, upstream step of data utilization.

- Ver: 01.a.20260917
- By: IWATA,Y.
- Mod: Initial creation

---

Target Audience
- All Tri-On Project stakeholders (particularly, those conducting on-site interviews)

---

Parent Document
- Tri-On-Edge Project Management

---

## The Five Questions

1. What is something you're finding troublesome or a bit of a hassle in your manufacturing-site work right now?

2. If that were gone, what would your work look like?

3. What kind of thing or information would help make that happen?

4. What information or data might be related to that?

5. Where is that information/data right now? Is it being collected, and can it be viewed together in one place?

- From what we've talked about today, is there anything small you could try first?

---

## Notes and Guidance

### 1. What is something you're finding troublesome or a bit of a hassle in your manufacturing-site work right now?

#### How to ask
　"Anything you can think of is fine — cost or time being lost, quality, delivery, safety, anything at all."

#### Points to check
- What is the cost/time being lost?
- Where does this fit in terms of OEE (availability, performance, quality rate) or QCD+S (Quality, Cost, Delivery, Safety)?
- Why does this happen, and why does it continue?
- Who else — other people or other departments — is troubled by this?


### 2. If that were gone, what would your work look like?

#### How to ask
　"Something like 'someone would find it easier / faster' is enough of an image."

#### Points to check
- Is it clear who, when, where, what, and how much?
- Is it framed as "the state achieved after acting," rather than "the action to take"?


### 3. What kind of thing or information would help make that happen?

#### How to ask
　"Visualization, an automatic way of noticing something, a judgment made by AI — anything you can think of is fine."

#### Points to check
- With [means], who becomes able to do what, when, where, and how much?


### 4. What information or data might be related to that?

#### How to ask
　"Either the result (good/bad, whether a failure occurred, etc.) or something that might be a cause (people, machines, materials, methods, environment, etc.) is fine."

#### Points to check
- Objective variable: outcome data (quality good/bad, equipment failure or not, amount of labor, etc.)
- Explanatory variables (candidates): candidate cause data (4M1E: Man, Machine, Material, Method, Environment, etc.)
- Are candidates being considered broadly, even if only tentative?


### 5. Where is that information/data right now? Is it being collected, and can it be viewed together in one place?

#### How to ask
　"If it's being collected, please share what you know — where it is, and roughly how fine-grained or how often it's gathered."
　"It's fine if it isn't being collected yet. How about we think about 'where it is right now' instead?"

#### Points to check
- Where does the data originate? When does it occur?
- Where is it collected/stored? When is it collected?
- How fine-grained (granularity) is the data?


### Closing & Next Action
- From what we've talked about today, is there anything small you could try first?

#### Points to check
- Is it concrete enough to be carried out quickly, on a small scale?

---

## Example Cases

### Case 1: Early Detection of Sudden Equipment Failure

#### **① The trouble**
　Equipment sometimes breaks down suddenly, stopping the line. Since no one knows when it will break, they always have to be on guard.

Sample checks
- **[OEE/QCD+S]**
  - Reduced "Performance," plus "Safety" (line-wide availability drops due to a sudden stop). Also spills over into Cost (labor and parts for the emergency response).
- **[Why it happens and continues]**
  - There is no way to notice the warning signs, so the operation only responds after a breakdown occurs (reactive maintenance).
- **[Who else is troubled]**
  - Maintenance staff (kept busy responding) / production control (plans get disrupted) / operators (dealing with sudden stops).

#### **② What would change if it were solved**
　Maintenance staff would know "something's about to go wrong" before it actually breaks, letting them calmly prepare parts or fold it into the weekend's planned maintenance. The line would no longer need to stop suddenly.

#### **③ What would help**
　A mechanism that gives early warning that the equipment's condition is turning bad. Something like: if the vibration or internal temperature starts behaving differently than usual, it gets noticed.

#### **④ Data that might be related**
　Outcome side: whether or not a failure occurred, when it occurred
　Cause (candidate) side: magnitude of vibration, internal equipment temperature, run time, type of product being produced (the load may differ)

Sample checks
- **[4M1E (candidate causes)]**
  - Machine (aging, part wear), Method (maintenance timing is fixed). Man, Material, and Environment seem only weakly related in this case.

#### **⑤ Is it being collected now?**
　It's presumably flowing in real time inside the PLC, but it isn't being sent to a server. Most likely, the PLC screen is only looked at when something has just broken.

Sample checks
- **[Collection point / storage location]**
  - Adding a data-collection mechanism would make the data in ④ available.
- **[Data granularity / frequency]**
  - Inside the PLC it's real-time (millisecond-to-second order), but the granularity and interval for server-side storage needs to be considered.

#### **Next action**
　Start with just one machine: pull vibration and temperature data from the PLC and send it to a server, and see what the waveform looks like.

---

### Case 2: Early Detection of Quality Degradation

#### **① The trouble**
　As equipment condition worsens, product quality gradually degrades, and sometimes an out-of-spec defect isn't noticed until it has already occurred.

Sample checks
- **[OEE/QCD+S]**
  - Reduced "Quality Rate" (quality). Increased cost from rework/scrap; if a defect ships out, it also spills over into delivery and trust.
- **[Why it happens and continues]**
  - There is no way to view the quality indicator continuously, so it's only assessed by pass/fail judgment against spec (seen as a single point, not a trend).
- **[Who else is troubled]**
  - Operators (keep producing defects) / quality assurance (dealing with shipped defects) / production control (worsening yield).

#### **② What would change if it were solved**
　Operators and line supervisors would notice "quality seems to be dropping" before it goes out of spec, and could adjust the equipment or change the setup. At minimum, they would notice the instant an out-of-spec defect occurs, and stop before producing more.

Sample checks
- **[The achieved state]**
  - A three-part set: collect → judge → output to the site (a physical device).

#### **③ What would help**
　They want to know the quality indicator is dropping even without looking at a computer. Something like a patrol lamp beside the line — naturally visible just by being there — to signal it.

#### **④ Data that might be related**
　Outcome side: the quality indicator's measured value (within spec or out of spec), timing of defect occurrence
　Cause (candidate) side: equipment operating data (vibration, temperature, pressure, etc.), the trend of the quality indicator itself (is it continuously declining?)

Sample checks
- **[4M1E (candidate causes)]**
  - Machine (equipment trouble, aging), Method (timing of adjustment/setup change). Man, Material, and Environment have room to confirm through further interviews.

#### **⑤ Is it being collected now?**
　Both the quality-indicator sensor values and the equipment operating data flow in real time inside the PLC. But since they aren't sent to a server, there's no way to look back at the trend afterward.

Sample checks
- **[Data granularity / frequency]**
  - Inside the PLC it's real-time (millisecond-to-second order), but it isn't being collected to a server.

**Next action**
　Start by pulling just the quality-indicator data from the PLC, and check on a graph whether a continuous decline is visible before it goes out of spec. If it is visible, consider a mechanism to connect it to the patrol lamp.

Sample checks
- **[Staged goal-setting]**
  - "Early detection while still within spec" = the ideal form / "detection right after going out of spec" = a realistic first step.
- **[Constraint on where feedback goes]**
  - Given that the site can't always watch a PC screen, an important design constraint is that the output destination is a physical device (such as a patrol lamp), not a PC notification.


End of document
