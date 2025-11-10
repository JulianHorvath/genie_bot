# CI/CD practical implementation guide for Databricks Apps (DABs and GitHub Actions)

### This guide shows how to quickly set up a CI/CD pipeline for Databricks Apps using Databricks Asset Bundles (DABs) and GitHub Actions, Allowing automatic deployment within a production environment.

--------------------------------------------------------------------------------

1. Prerrequisites\
Before starting, make sure you have installed and configured following dependencies and credentials:

   - Python 3.11 or later.
   - Databricks CLI 0.18.0 or later.
   - U2M OAuth authentication configured for your workspace: [OAuth user-to-machine authentication](https://docs.databricks.com/aws/en/dev-tools/cli/authentication#oauth-user-to-machine-u2m-authentication).

2. Initial project set up and mandatory files\
Included workflows will allow local testing, on-demand and fully automated deployments to a Databricks production environment.

   - Clone the example repository: start a fork from original one, cloning to your local machine.
   - Key project files: to define the application, resources and workflows.
     - src/app/**app.py**: contains app source code.
     - src/app/**requirements.txt**: Lists all necessary dependencies for your application.
     - resources/**app.yml**: Specifies app configurations at a resource level, defining source code path (../src/app) and Service Principal permissions over app resources.
     - **databricks.yml**: Asset Bundle definition, specifies environments (dev y prod), their host URLs, and bundle specific configuration for each environment.
     - ./github/workflows/**push.yml**: define Continuous Integration (CI) workflow. Triggered when pull requests (PRs) or pushes to main branch.
     - ./github/workflows/**release.yml**: define Continuous Development workflow. Triggered when new GitHub release.

3. Development and Local Testing (Step-by-Step)\
This process allows developing and testing your application before sending it to Databricks.
- Task 1: Configure a Python Virtual Environment and install dependencies:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r src/app/requirements.txt
```

- Task 2: Run app locally. You can make any change needed at this stage:

```
streamlit run src/app/app.py
```

- Task 3: Run Unit Tests. Repository includes simple testing case at src/app/tests/test_app.py. Execute it with pytest:

```
python -m pytest
```

- Tip: Same tests will be later run automatically with GitHub Actions.

4. Production Environment and Deployment Configuration\
Example bundle defines two environments: dev (development) and prod (production).

- Task 1: Configure databricks.yml . Update databricks.yml file with your own workspace URLs for dev and prod targets:

```
targets:
  dev:
    # ... development configuration
    workspace:
      host: <your-workspace-url> # Switch to your host URL
  prod:
    # ... production configuration
    workspace:
      host: <your-workspace-url> # Switch to your host URL
```

Tip: For this guide you can assign same workspace as target dev and prod. It is recommendes using separated workspaces for them.

- Task 2: Deploy resources to development using Databricks CLI (should be already authenticated with OAuth). Deploys the bundle within development environment:

```
databricks bundle deploy -t dev
```

This command creates compute resource in Databricks Apps and uploads project code to the workspace.

- Task 3: Initialize the app and deploy to dev environment. Run:

```
databricks bundle run hello-world-app -t dev
```

Confirm the app is running as expected in dev environment before continue.

5. Prepare automatic deployment (Production)\
Automatic deployment to production using GitHub Actions requires a Service Principal (SP) to authenticate safely.

- Task 1: Configure Service Principal (SP)
  -  Create a Service Principal for production environment.
  -  Create an OAuth secret for the SP and take note of its client ID and client secret.

- Task 2: Create GitHub secrets.\
In your GitHub repository, create three secrets that GitHub Actions can later access:
  - **DATABRICKS_CLIENT_ID**: Service Principal client ID.
  - **DATABRICKS_CLIENT_SECRET**: Service Principal client secret.
  - **DATABRICKS_HOST**: Databricks workspace URL (e.g. https://my-workspace.cloud.databricks.com/).

6. GitHub Actions configuration\
Example project includes two GitHub Actions workflows that allows CI/CD.

**CI Workflow**: push.yml (Continuous Integration)
This flow is triggered every time there is a Pull Request or Push to main branch. Its goal is to ensure code quality and successfull testing before merging.

Tasks at push.yml: this workflow includes steps for:
   - Configure Python 3.11.
   - Install dependencies (app/requirements.txt).
   - Install quality tools (ej: pytest, ruff, streamlit).
   - Run ruff for linting.
   - Run unit tests using python -m pytest.

**CD Workflow**: release.yml (Continuous Deployment to Production)
This flow is triggered every time there is a release in GitHub. Makes deployment into production using repository secrets configured in point 5.

Tasks in release.yml: This workflow includes two jobs that use databricks bundle command and target prod environment:
   - deploy: Create compute resource in Databricks Apps if not exists, using databricks bundle deploy command.
   - update_app: Depends on deploy job and triggers a new code deployment using databricks bundle run hello-world-app command.

Once configured, you can implement changes in code that will trigger CI workflow and create releases that will trigger deployment to production.

- Task 1: Implement changes and trigger CI
  - Create dev branch and apply changes (e.g. en src/app/app.py).
  - Create Pull Request (PR): Creates a PR to merge dev branch with main branch. This will automatically trigger push.yml (CI) workflow.
  - Merge PR: Once quality tests are passed, merge pull request into main branch.

- Task 2: Create a Release and trigger CD to Production\
To deploy changes into production, create a new release:
  - Create a Git tag in main branch:
  - Create a Release in GitHub: Use GitHub UI to create new release and select recently created tag (e.g.: v.0.1.0).

Release creation will trigger workflow defined in release.yml (CD). Once completed successfully, new app version will be executing in your prod environment.