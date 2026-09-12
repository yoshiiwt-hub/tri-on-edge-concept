# Tri-On-Edge Stream Architecture

　This document presents the basic architectural policy for Tri-On-Edge Stream (the manufacturing-site stream data processing infrastructure).

　This policy itself will keep watching and evaluating the latest technology trends, and will be grown through continuous practical application, evaluation, and improvement (Trial).

　It also follows the principle of not starting from technology (the means): thinking in the order of Will (what you want to do, the ideal state, the state you should be in) → Rule (documented rules, standards) → Tool (means, technology, execution method) (Will 1st, Rule 2nd, Tool Later).

- Ver: 03.a.20260814
- By: IWATA,Y.
- Mod: Document restructuring

---

Target Audience
- Developers

---

Parent Document
- Tri-On-Edge Development Concept

---

## 1. Selection of Base Technologies

### 1. Development Language

　**Python** is used as the development language.

#### Rationale

　The following properties allow the Trial/Trinity improvement cycle to be run quickly and smoothly.

  - Widely documented on the web, making information easy to gather. As a result, there is a large pool of capable people and vendors, and AI-assisted code/documentation generation works effectively.
  - As an interpreted language that runs directly from a script, it enables a fast implementation–verification–fix cycle.
  - A wide variety of external libraries are available as OSS, allowing efficient support for the many communication methods and network protocols found at manufacturing sites.
  - Its cross-platform support allows it to run on the many different hardware and OS combinations found at manufacturing sites (PLCs, box PCs, servers).

#### Python Version and Bit Width

- Python 3.11 (64-bit) or later is the baseline. However, Python 3.10 / 32-bit is allowed depending on the existing environment.

  - This is to accommodate the wide variety of environments found at manufacturing sites, including embedded equipment.

~~~text
python = ">=3.10" # 64bit recommended. 32bit allowed as last resort 
~~~

  - OS: Windows / Linux
  - CPU architecture: Intel / ARM
  - CPU bit width: 64-bit / 32-bit

#### Notes
  - Because Python is an interpreted language, there are challenges in stable operation that requires strict control of computational performance and memory (the GIL: Global Interpreter Lock).
  - For deployment to the runtime environment, use techniques such as conversion to C to address computational performance and memory management (see "3. Runtime Environment (Build / Deploy)" below).


### 2. In-System Data Storage

#### Files (Configuration Files)

- **JSON (.json)** is used.
  - It can be handled by Python's standard library and maps naturally onto Python dictionaries.
  - It is a de facto standard, with abundant tooling support (editors, etc.).

#### Internal System DB
  
- **SQLite** is used.
  - It can be handled by Python's standard library and works as a simple, single-file setup.
  - Performance should be considered, e.g., using WAL mode to reduce read/write contention.
  - Storage write-lifetime should be considered, e.g., placing the journal/temp store in MEMORY and relaxing synchronization (deferring to the OS buffer).
  
#### AI Model I/O and Inference Execution

- **ONNX** and **ONNX Runtime** are used.
- However, methods that can run with a small enough set of human-manageable parameters — such as the MT method (Mahalanobis-Taguchi) or multiple regression — may be managed as configuration files instead.


### 3. In-System Data Exchange

#### Intra-Process Communication
  
- The Python standard library **queue.Queue** is used.
  - Message queuing enables asynchronous, resource-efficient data processing.
  - It helps suppress data loss.

#### Inter-Process Communication
  
- **Socket communication** is used.
  - It requires no additional libraries beyond a Python environment, keeping the setup simple.

- When performance or robustness is required, the external Python library **ZeroMQ (ipc:// / tcp://)** is used.
  - Its brokerless, simple architecture enables lightweight and robust communication.

#### Data Exchange Format
  
- **JSON** or **pickle** is used. **(TBD: MessagePack is being considered for future use.)**
  - Both require no additional libraries beyond a Python environment, keeping the setup simple.

- Note: pickle may only be used temporarily, within the system and during communication, and must never be persisted to a file, due to version-incompatibility and security-vulnerability concerns.


### 4. Data Exchange with External Systems

- **Parquet** is used.
  - It has typed data, avoiding data-type-checking problems.
  - It is a binary format, avoiding text-encoding problems.
  - It is columnar, making it easy for downstream systems to read only the columns they need.

- **CSV** may be used when the data needs to be shown to a person.
  - It requires no additional libraries beyond a Python environment, keeping the setup simple.
  - It is a de facto standard that can be intuitively viewed using tools already established at the site, such as Excel and Notepad.

- **JSON** may be used only when its structure can be managed.
  - Example: when exchanging data with an external system together with a JSON Schema.
  - Freely-structured JSON requires a large amount of validation effort.

- When storing data to the cloud, **Hive-style** partitioning must be supported.
  - It offers excellent big-data queryability and is a de facto standard on the major cloud platforms (AWS, Azure, GCP).


### 5. System Management

- For notification, record-keeping (management ledgers), file version history/backup, and terminal distribution, integration with an established internal environment already in use on-site should be considered.

Examples
  - Notification via a chat tool such as Microsoft Teams
  - A management ledger using a tool such as Microsoft SharePoint (SPO)
  - File version control, backup, and distribution via Git


## 2. Software Structure
　To realize the core concepts of Trigger (detecting points of change at the manufacturing site), Trial (practice and verification through Genchi Genbutsu — the actual place, the actual thing, the actual data), and Trinity (coordination), the following defines the software structure.

### Loosely-Coupled Module (File) Structure

Based on the 4S / 5 Fixed Points way of thinking, Tri-On-Edge adopts a **loosely-coupled structure** that clearly separates "who touches what, for what purpose." This optimizes the timing of change/reference (the operational phase), the frequency of change, and who makes the change.

- **Site personnel** only need to touch the configuration files.
- **Trigger developers** only need to edit the Trigger modules.
- **Core developers** only need to manage the infrastructure.
- **Administrators of other systems** only need to handle the control files needed for system integration.

#### 1. Core Part

- The core infrastructure modules that form the main line of the data pipeline, such as Stream Core, Supervise Base, and Operator.
- Cannot be changed after distribution to the site (changes require rebuilding and redistributing from the development environment).

#### 2. Trigger Part

- Python scripts that are dynamically loaded via `importlib` (described below).
- Can be directly edited/replaced on-site during the Trial phase.
- Logic can be changed without rebuilding the Core.
- Note: adding a new external library still requires rebuilding the Core layer.

#### 3. Configuration Part

- Specifies the runtime parameters for the Core and Trigger layers (connection targets, thresholds, save destinations, polling intervals, etc.).
- Absorbs differences between deployment sites through configuration-file changes alone.

#### 4. Control Part (Control Files)

- Controls things such as system shutdown, offline state, and debug mode, via the presence/absence and modification time of specific control files.
- Allows the system to be controlled through file operations alone, without a GUI or network connection.


### Dynamic Loading of Triggers

　Rather than hard-coding input/collection (Find Trigger) and output/post-processing (Act Trigger), Stream Core dynamically loads what is needed at startup (a Trigger Set).

- Fast Try & Error: a Trigger developer only needs to create a single Python file (the new Trigger) and update the configuration file to immediately test it on-site (Trial).
- Protection of core logic: adding a new input source (e.g., a new PLC device) or output destination (e.g., a new cloud database) requires no modification to core infrastructure modules such as Stream Core.
- Memory and resource optimization: since only the Trigger modules needed at runtime are loaded, this prevents memory pressure from unused, heavy libraries and makes efficient use of the edge PC's limited resources (avoiding Muda/waste).

Mechanism
- **Dynamic binding** (`importlib`): the "module name" and "function name" written in the configuration file are received as strings, and the corresponding worker function is launched as a thread via `importlib.import_module()` and `getattr()`.

- **Configuration-file-driven**: runtime parameters are read from `trigger_setting_01.json` / `trigger_setting_02.json` at startup.


## 3. Runtime Environment (Build / Deploy)

　Build and deployment (distribution to the runtime environment) follow the policy below.

#### Portable Environment (Zero-Install Operation)

　Tri-On-Edge is deployed so that it can run simply by placing the designated files/folders in the runtime environment, without requiring a network or internet connection.

- **No environment setup work or network connection needed on the site PC**: no installation work or online updates are required on the site PC.
- **Environment isolation**: minimizes the risk of contaminating or breaking the environment of other control software running on the target PC.
- **Fast recovery**: recovery can be completed in minutes simply by overwriting with a backed-up copy of the "Tri-On-Edge folder."

#### Three Execution Stages

　Tri-On-Edge uses the execution format best suited to the operational phase (development, trial, or mass production).

- Development stage
  - Run the `.py` files directly, using a portable Python environment such as WinPython (on Windows).
  - Code can be edited and executed immediately.

- Verification (Trial) stage
  - Run a packaged `.exe` built with PyInstaller.
  - The Trigger (input/output) portions can be edited/replaced on-site.

- Production (mass-production) stage
  - Run an `.exe` binary module built via Nuitka, which compiles through C.
  - This improves execution speed and operational robustness/stability (memory management, etc.).


End of document
