## Why?

How to scale when dealing with tens of million records growing exponentially every day, other than Power BI/Tableau/Looker...? How to make complex big data interpretable and discoverable for all user profiles?

As much as you put your effort and energy on deliver a quick and complete easy-to-use BI dashboard, it becomes obvious is not you, is the tool. And fundamentally, the purpose. Changes create unsafe feelings, for sure, but are a must to overcome any limitation. 'Limits, like fears, are often just an ilussion'.

As soon as you got it, something new is born. Then implementation is just a matter of picking the right frameworks and recover other experiences to inspire unsort thoughts that need to be ordered, so you can achieve exactly what you are looking for.

## Acknowledgment disclaimer

[Databricks Sandbox Conversational App](https://github.com/databrickslabs/sandbox/blob/main/conversational-agent-app/genie_room.py) repo hosted by Vivian Xie was the spark that ignited everything done here. Not only providing some code examples, but also and more importantly, a logical approach to get the goal.

## Structure

How we tried to accomplish ours? There is a pretty ilustrative image that highlights some essential components behind a well executed digital product:

![Digital Product Backbone](./assets/Digital_Product_Backbone.png)

They were handled as follows:

* Data: there is a previous and extensive job on developing necessary pipelines to create a curated data warehouse that can be queried efficiently. SQL and Pyspark are the silent heros behind everything. Working in a distributed system like Apache Spark makes partitioning/clustering strategy critical for fast retrieving.
* LLM: Genie underlying Serving End Point.
* Communication: Databricks Python SDK.
* Authentication: Databricks Personal Access Token.
* UX/UI: Streamlit and Genie features. ![Open sample](./assets/UI_Sample.png)
* Repository: GitHub.
* Automation: Pytest + Databricks Asset Bundles + GitHub Actions (take a look at CI_CD_notes.md file).
* Hosting and Deployment: Databricks Apps.

## AI and Vibecoding

* Based on genie_room module and Databricks REST API / Python SDK docs, core functions were cleaned and capabilities extended, plus created new ones, to cover most of app features and use cases.
* Next, learned from scratch Streamlit by following its chatbot tutorials and example gallery (Streamlit's ask AI assistant stood big when it mattered). After some time, a pretty decent prototype script was landed, which main target was a correct interaction between mentioned module and Streamlit concepts and objects.
* At this point and to speed up the development, iteration over the code was confronted with the help of some other unknown assistants (ChatGPT, Gemini). Using a testing Genie Space to understand possibilities, breaking points and overall dynamic between Databricks' client calls and Streamlit's session_state, it took very limited computation and time to create tests and pass them to make the application ready for production.
* Finally, a CI/CD pipeline with minimalist dev/prod git structure has been implemented, taken some notes from [Databricks Apps Cookbook](https://apps-cookbook.dev/blog/automate-apps-deployments-dabs/) hosted by Pascal Vogel and Antonio Samaniego Jurado. Passing the cookbok and its related GitHub repo example, NotebookLM finished it off delivering a step-by-step, safe implementation handbook (see CI_CD_notes.md and assets/CI_CD Mind Map.png).

## Others

* At the core of Text-to-"something" applications there is a strong focus on non-technical users in the pursuit for AI expansion. Thinking about that, a text-to-sql guide was included, generated with AI and few-shot prompting that can be downloaded straight from the app's UI.
* A dockerfile was added for technical users, as other option to run the app.
* Because NDA, neither a DER nor any other documentation specific to the client's data was included.

## Key takeaways:

### Blockers

* Lacked of necessary level permissions and accesses to configure the underlying Genie Serving Endpoint model programatically, but it is absolutely possible to serve your self model. Hugging Face has already some text-to-sql fine-tuned base models but it is interesting to fine-tune yours with proprietary data. This is strongly recommended to gain more control and being away of vendor dependency. This task was ultimately handed by passing contextual business information through Genie UI settings (system prompt, metrics, columns synonyms, etcetera).
* Backend relies on a specific vendor. Although there are plenty of them with similar frameworks, for full control it is preferred building your own API, which allows deploying seamlessly anywhere.
* As of today, there is no endpoint/sdk method or argument to pass an attachment when creating a message, to replicate same UX as in Genie IDE. However, necessary functions and features to include attachments when prompting were included, hoping the method/argument will be available soon. Same case applies to user feedback.
* As of today, there is a missmatch between docs and server regarding assistant response, so you can only retrieve Genie's description and SQL query generated code, but not the natural language answer.

### Next steps

* Include a DB to persist conversations and messages, rather than calling API/caching results each time.
* MCP registration can leverage communication to external systems as information sources. For example, Atlassian's MCP for Confluence, which is a regular tool where projects documentation is stored.
* Build an Analytics Dashboard to track app usage and billing through Databricks' system tables: [Monitor App Costs](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/monitor#monitor-app-costs).
* Conversations can't be renamed through Genie IDE, so they will naturally show as original questions in the app. However, we explored but not included yet a solution using ai_summarize() Databricks SQL function with DB persistance, which can improve UX.

## Project summary:

### Pros: 

* Scalability from on-demand Spark computation
* UX/UI
* Managed access/authentication
* ROI can be calculated using Databricks system tables

### Cons: 
* Vendor dependency
* Programatic customization without permissions/accesses
* Some latency on API calls, particularly when loading chat history

* Ongoing API/SDK development can cost breaks and missfunctionalities, but this should be transitory

