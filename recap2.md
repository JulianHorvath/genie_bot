## Version 2 Enhancements:

* DB layer: reduced latency loading chat history using initial bulk load plus session_state memory
* Fault tolerance: handled storage and connection errors using queue logic + offline batch job
* UX: improved by generating AI summarized titles for chats
* Cost/Compute: regenerated/updated queries results, more previsible costs running on top of SQL Warehouse instead of LLM endpoint
* Observability: produced idempotented results by re-executing always same queries that already worked
* Results size: delivered DF through EXTERNAL_LINKS + CSV alternative option for large results
* Analytics: enabled analytics sql_run_version and user/session information
* Reusability: added similarity search enables synergy between users and teams, no need to reinvent the wheel
* Testing: simplified tests for GenieClient and app Integration (real app flow)
* Business use case: can run app for any Genie Space, isolating a data model w/business logic (e.g. data marts)
* Flexible for each team to manage its own data model/subdomain (tables, joins, metrics, etc.) through Genie Spaces

## Future improvements:

* attachment_id to bring attachment DataFrame before expiration (12 hours expiration time)
* Overcome file retrieval limit for results downloading
* Evolve architecture into agentic AI