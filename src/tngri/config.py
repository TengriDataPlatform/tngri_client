import dataclasses
import types

import click

DEFAULT_SQL_PORT = 5433
DEFAULT_RPC_PORT = 57776


def click_arguments(cls):
    if not dataclasses.is_dataclass(cls):
        raise ValueError("provided object is not a dataclass")

    def wrapper(f):
        for field in dataclasses.fields(cls)[::-1]:
            field_type = field.type
            field_name = field.name
            required = False
            if isinstance(field_type, types.UnionType):
                field_type = field_type.__args__[0]
            else:
                required = True
            click_name = "--" + field_name.replace("_", "-")
            is_flag = field.type is bool

            f = click.option(
                click_name,
                is_flag=is_flag,
                type=field_type,
                default=field.default,
                envvar="TNGRI_" + field_name.upper(),
                required=required,
                show_envvar=True,
            )(f)

        return f

    return wrapper


@dataclasses.dataclass()
class BaseConfig:
    debug: bool = False

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    @classmethod
    def from_click_options(cls, ctx):
        params = {k: v for k, v in ctx.params.items() if v is not None}
        return cls(**params)

    @classmethod
    def click_arguments(cls):
        return click_arguments(cls)

    @classmethod
    def from_env(cls):
        @click.command(
            add_help_option=True,
            context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
        )
        @click.pass_context
        @cls.click_arguments()
        def arguments(ctx, *args, **kwargs):
            return ctx

        try:
            ctx: click.Context = arguments(standalone_mode=False)
        except click.exceptions.MissingParameter as e:
            raise ValueError(f"{e.param} is missing") from e
            exit(1)

        return cls.from_click_options(ctx)


@dataclasses.dataclass
class S3Config(BaseConfig):
    s3_region: str = dataclasses.field(default="eu-central-1", metadata=dict(additional_env="AWS_DEFAULT_REGION"))
    s3_default_region: str = dataclasses.field(default="eu-central-1", metadata=dict(additional_env="AWS_DEFAULT_REGION"))
    s3_access_key_id: str = dataclasses.field(default=None, metadata=dict(additional_env="AWS_ACCESS_KEY_ID"))  # type: ignore
    s3_secret_access_key: str = dataclasses.field(default=None, metadata=dict(additional_env="AWS_SECRET_ACCESS_KEY"))  # type: ignore
    s3_endpoint_url: str = dataclasses.field(default=None, metadata=dict(additional_env="AWS_ENDPOINT_URL"))  # type: ignore
    s3_bucket_name: str = None  # type: ignore


@dataclasses.dataclass
class Config(S3Config):
    pass
