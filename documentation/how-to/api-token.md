# Provide your API Token

Reading data from HDX does not need a token. Writing data does. To create one, follow
the instructions in
[HDX core concepts](https://docs.humdata.org/build/overview/hdx-core-concepts).

Supply the token to the library (called `hdx_key`) in one of these ways:

1. Pass it as a parameter or within a dictionary
2. Set the `HDX_KEY` environment variable
3. Put it in a JSON or YAML file. The default path is **.hdx_configuration.yaml** in
   the current user's home directory **\~**. Then put in the YAML file:

        hdx_key: "HDX API TOKEN"
