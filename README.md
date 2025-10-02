# buzzline-05-case

Nearly every streaming analytics system stores processed data somewhere for further analysis, historical reference, or integration with BI tools.

In this example project, we incorporate relational data stores. 
We stream data into SQLite and DuckDB, but these examples can be altered to work with MySQL, PostgreSQL, MongoDB, and more.

We use one producer that can write up to four different sinks:

- to a file
- to a Kafka topic (set in .env)
- to a SQLite database
- to a DuckDB database

In data pipelines:

- A **source** generates records (our message generator). 
- A **sink** stores or forwards them (file, Kafka, SQLite, DuckDB). 
- An **emitter** is a small function that takes data from a source and writes it into a sink. 
- Each emitter has one job (`emit_message` to the specified sink). 

Explore the code to see which aspects are common to all sinks and which parts are unique.

--- 

## First, Use Tools from Module 1 and 2

Before starting, ensure you have completed the setup tasks in <https://github.com/denisecase/buzzline-01-case> and <https://github.com/denisecase/buzzline-02-case> first.
**Python 3.11 is required.**

## Second, Copy This Example Project & Rename

1. Once the tools are installed, copy/fork this project into your GitHub account
   and create your own version of this project to run and experiment with.
2. Name it `buzzline-05-yourname` where yourname is something unique to you.

Additional information about our standard professional Python project workflow is available at
<https://github.com/denisecase/pro-analytics-01>. 
    
---

## Task 0. If Windows, Start WSL

Launch WSL. Open a PowerShell terminal in VS Code. Run the following command:

```powershell
wsl
```

You should now be in a Linux shell (prompt shows something like `username@DESKTOP:.../repo-name$`).

Do **all** steps related to starting Kafka in this WSL window.

---

## Task 1. Start Kafka (using WSL if Windows)

In P2, you downloaded, installed, configured a local Kafka service.
Before starting, run a short prep script to ensure Kafka has a persistent data directory and meta.properties set up. This step works on WSL, macOS, and Linux - be sure you have the $ prompt and you are in the root project folder.

1. Make sure the script is executable.
2. Run the shell script to set up Kafka.
3. Cd (change directory) to the kafka directory.
4. Start the Kafka server in the foreground. Keep this terminal open - Kafka will run here.

```bash
chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

**Keep this terminal open!** Kafka is running and needs to stay active.

For detailed instructions, see [SETUP_KAFKA](https://github.com/denisecase/buzzline-02-case/blob/main/SETUP_KAFKA.md) from Project 2. 

---

## Task 2. Manage Local Project Virtual Environment

Open your project in VS Code and use the commands for your operating system to:

1. Create a Python virtual environment.
2. Activate the virtual environment.
3. Upgrade pip and key tools. 
4. Install from requirements.txt.

### Windows

Open a new PowerShell terminal in VS Code (Terminal / New Terminal / PowerShell).
**Python 3.11** is required for Apache Kafka. 

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt
```

If you get execution policy error, run this first:
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Mac / Linux

Open a new terminal in VS Code (Terminal / New Terminal)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```

---

## Task 3. Run Tests and Verify Emitters

In the same terminal used for Task 2, we'll run some tests to ensure that all four emitters are working fine on your machine.  All tests should pass if everything is installed and set up correctly. 

```shell
pytest -v
```

Then run the `verify_emitters.py` script as a module to check that we can emit to all four types. 
For the Kakfa sink to work, the Kafka service must be running. 

### Windows Powershell

```shell
py -m verify_emitters
```

### Mac / Linux

```shell
python3 -m verify_emitters
```

---

## Task 4. Start a New Streaming Application

This will take two terminals:

1. One to run the producer which writes messages using various emitters. 
2. Another to run each consumer. 

### Producer Terminal (Outputs to Various Sinks)

Start the producer to generate the messages. 

The existing producer writes messages to a live data file in the data folder.
If the Kafka service is running, it will try to write the messages to a Kafka topic as well.
For configuration details, see the .env file. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.\.venv\Scripts\Activate.ps1
py -m producers.producer_case
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.producer_case
```

NOTE: The producer will still work if the Kafka service is not available.

### Consumer Terminal (Various Options)

Start an associated consumer. 
You have options. 

1. Start the consumer that reads from the live data file.
2. Start the consumer that reads from the Kafka topic.
3. Start the consumer that reads from the SQLite relational data store. 
4. Start the consumer that reads from the DuckDB relational data store.

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.\.venv\Scripts\Activate.ps1
py -m consumers.kafka_consumer_case
OR
py -m consumers.file_consumer_case
OR
py -m consumers.sqlite_consumer_case
OR
py -m consumers.duckdb_consumer_case
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.kafka_consumer_case
OR
python3 -m consumers.file_consumer_case
OR
python3 -m consumers.sqlite_consumer_case.py
OR
python3 -m consumers.duckdb_consumer_case.py
```

---

## Review the Project Code

Review the requirements.txt file. 
- What - if any - new requirements do we need for this project?
- Note that requirements.txt now lists both kafka-python and six. 
- What are some common dependencies as we incorporate data stores into our streaming pipelines?

Review the .env file with the environment variables.
- Why is it helpful to put some settings in a text file?
- As we add database access and passwords, we start to keep two versions: 
   - .env 
   - .env.example
 - Read the notes in those files - which one is typically NOT added to source control?
 - How do we ignore a file so it doesn't get published in GitHub (hint: .gitignore)

Review the .gitignore file.
- What new entry has been added?

Review the code for the producer and the two consumers.
 - Understand how the information is generated by the producer.
 - Understand how the different consumers read, process, and store information in a data store?

Compare the consumer that reads from a live data file and the consumer that reads from a Kafka topic.
- Which functions are the same for both?
- Which parts are different?

What files are in the utils folder? 
- Why bother breaking functions out into utility modules?
- Would similar streaming projects be likely to take advantage of any of these files?

What files are in the producers folder?
- How do these compare to earlier projects?
- What has been changed?
- What has stayed the same?

What files are in the consumers folder?
- This is where the processing and storage takes place.
- Why did we make a separate file for reading from the live data file vs reading from the Kafka file?
- What functions are in each? 
- Are any of the functions duplicated? 
- Can you refactor the project so we could write a duplicated function just once and reuse it? 
- What functions are in the sqlite script?
- What functions might be needed to initialize a different kind of data store?
- What functions might be needed to insert a message into a different kind of data store?

---

## Explorations

- Did you run the kafka consumer or the live file consumer? Why?
- Can you use the examples to add a database to your own streaming applications? 
- What parts are most interesting to you?
- What parts are most challenging? 

---

## Verify DuckDB (Terminal Commands)

Windows PowerShell

```shell
# count rows
duckdb .\data\buzz.duckdb "SELECT COUNT(*) FROM streamed_messages;"

# peek
duckdb .\data\buzz.duckdb "SELECT * FROM streamed_messages ORDER BY id DESC LIMIT 10;"

# live analytics
duckdb .\data\buzz.duckdb "SELECT category, AVG(sentiment) FROM streamed_messages GROUP BY category ORDER BY AVG(sentiment) DESC;"
```

macOS/Linux/WSL

```shell
# count rows
duckdb data/buzz.duckdb -c "SELECT COUNT(*) FROM streamed_messages;"

# peek
duckdb data/buzz.duckdb -c "SELECT author, COUNT(*) c FROM streamed_messages GROUP BY author ORDER BY c DESC;"

# live analytics
duckdb data/buzz.duckdb -c "SELECT category, AVG(sentiment) FROM streamed_messages GROUP BY category ORDER BY AVG(sentiment) DESC;"

```

---

## How To Stop a Continuous Process

To kill the terminal, hit CTRL c (hold both CTRL key and c key down at the same time).

## Later Work Sessions

When resuming work on this project:

1. Open the project repository folder in VS Code. 
2. Start the Kafka service (use WSL if Windows) and keep the terminal running. 
3. Activate your local project virtual environment (.venv) in your OS-specific terminal.
4. Run `git pull` to get any changes made from the remote repo (on GitHub).

## After Making Useful Changes

1. Git add everything to source control (`git add .`)
2. Git commit with a -m message.
3. Git push to origin main.

```shell
git add .
git commit -m "your message in quotes"
git push -u origin main
```

## Save Space

To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later.
Managing Python virtual environments is a valuable skill.

## License

This project is licensed under the MIT License as an example project.
You are encouraged to fork, copy, explore, and modify the code as you like.
See the [LICENSE](LICENSE.txt) file for more.

## Recommended VS Code Extensions

- Black Formatter by Microsoft
- Markdown All in One by Yu Zhang
- PowerShell by Microsoft (on Windows Machines)
- Python by Microsoft
- Python Debugger by Microsoft
- Ruff by Astral Software (Linter + Formatter)
- **SQLite Viewer by Florian Klampfer**
- WSL by Microsoft (on Windows Machines)





### New Consumer — `consumer_strickland.py`: Reddit r/learnmath → Category Distribution (SQLite)

**Ingest Source:** Live Reddit comments from `r/learnmath` using `praw`  
**Insight:** Classify each new comment into math categories (algebra, calculus, geometry, statistics, precalculus, general) and update **category distribution** counts in SQLite.

**Why it’s interesting:** Shows which math topics dominate in real time—useful for planning lessons, targeted supports, and engagement analysis.

### What We Store
- `category_counts(category TEXT PRIMARY KEY, total_msgs INT, last_ts TEXT)`
- `messages_processed(id PK, ts, author, category, snippet)` (audit trail)

### Setup
1. Create a Reddit **script** app at https://www.reddit.com/prefs/apps to get `client_id` and `client_secret`.
2. Create `.env`:

# How to get client_id and client_secret (step-by-step)

Log in on desktop to your Reddit account.

Open Developer Apps:
go to https://www.reddit.com/prefs/apps (you must be logged in).

Create a new app

Scroll to Developed Applications → click create another app…

name: learnmath-consumer (anything)

type: select script

description: optional

about url: leave blank

redirect uri: put exactly http://localhost:8080

Click create app.

Where to find the two values after creation

On the app card you just created:

Under the app name, you’ll see a small line that shows “personal use script” and a short string just below it.
👉 That short string is your client_id.

On the same card you’ll see “secret” followed by a long string.
👉 That long string is your client_secret.

(You do not need your Reddit password or 2FA for read-only access. Just client_id, client_secret, and a user_agent string.)

Put them in your .env

Create or edit the file at your repo root: .env (no quotes around values):

REDDIT_CLIENT_ID=put_the_short_string_here
REDDIT_CLIENT_SECRET=put_the_long_secret_here
REDDIT_USER_AGENT=learnmath-category-consumer by u/YourRedditUsername
REDDIT_SUBREDDIT=learnmath
SQLITE_DB_PATH=data/insights.db


Tip: Don’t commit .env to GitHub. Keep secrets local. (If .env isn’t in .gitignore, add it.)

Make your code use read-only Reddit (no username/password)

In consumers/consumer_strickland.py, use this builder:

def build_reddit() -> praw.Reddit:
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT):
        raise RuntimeError("Missing client_id/client_secret/user_agent in .env")
    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        check_for_async=False,  # avoids warnings
    )


Remove any use of REDDIT_USERNAME / REDDIT_PASSWORD. You won’t need them.

Optional quick sanity check right after reddit = build_reddit():

print("read_only?", reddit.read_only)  # should print True

Run it

From your project folder with the venv active:

py -m consumers.consumer_strickland


You should see:

[consumer_molly] Streaming comments from r/learnmath → data/insights.db
[ok] r/learnmath | <author> → <category>
...


If you still see 401:

Double-check you created an app of type script (not “installed” or “web”).

Re-copy the short string under the app name into REDDIT_CLIENT_ID.

Re-copy the secret string next to the word secret into REDDIT_CLIENT_SECRET.

Make sure your .env has no quotes and no trailing spaces.