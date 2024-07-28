create-project:
	dagster project from-example --example project_dagster_university_start --name dagster_university
setup-envs:
	cd dagster_university \
	&& cp .env.example .env \
	&& pip install -e ".[dev]"
dev:
	cd dagster_university \
	&& dagster dev

