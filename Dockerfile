FROM public.ecr.aws/lambda/python:3.12

ARG LAMBDA_HANDLER=metadata_entry

WORKDIR /build
COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir --target "${LAMBDA_TASK_ROOT}" /build \
    && rm -rf /build /root/.cache

WORKDIR ${LAMBDA_TASK_ROOT}

RUN printf 'from growler.lambdas.%s import lambda_handler\n' "$LAMBDA_HANDLER" > handler.py

CMD ["handler.lambda_handler"]
