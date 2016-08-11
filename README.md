ExAws
=====
[![Build Status](https://travis-ci.org/CargoSense/ex_aws.svg?branch=master)](https://travis-ci.org/CargoSense/ex_aws)

A flexible easy to use set of AWS APIs.

- `ExAws.Dynamo`
- `ExAws.EC2`
- `ExAws.Kinesis`
- `ExAws.Firehose`
- `ExAws.Lambda`
- `ExAws.RDS`
- `ExAws.S3`
- `ExAws.SNS`
- `ExAws.SQS`

## 1.0.0-beta0 Changes

The `v0.5` branch holds the legacy approach.

ExAws 1.0.0 takes a more data driven approach to querying APIs. The various functions
that exist inside a service like `S3.list_objects` or `Dynamo.create_table` all
return a struct which holds the information necessary to make that particular operation.

You then have 4 ways you can choose to execute that operation:

```elixir
# Normal
S3.list_buckets |> ExAws.request #=> {:ok, response}
# With per request configuration overrides
S3.list_buckets |> ExAws.request(config) #=> {:ok, response}

# Raise on error, return successful responses directly
S3.list_buckets |> ExAws.request! #=> response
S3.list_buckets |> ExAws.request!(config) #=> response
```

Certain operations also support Elixir streams:

```elixir
S3.list_objects("my-bucket") |> ExAws.stream! #=> #Function<13.52451638/2 in Stream.resource/3>
S3.list_objects("my-bucket") |> ExAws.stream!(config) #=> #Function<13.52451638/2 in Stream.resource/3>
```

The ability to return a stream is noticed in the function's documentation.

### Migration

TL;DR:
Do this now:
```
ExAws.S3.get_object("my-bucket", "path/to/object") |> ExAws.request
```
not
```
ExAws.S3.get_object("my-bucket", "path/to/object")
```

This change greatly simplifies the ExAws code paths, and removes entirely the complex
meta-programming pervasive to the original approach. However, it does constitute
a breaking change for anyone who had a client with custom logic.

## Highlighted Features
- Easy configuration.
- Minimal dependencies. Choose your favorite JSON codec and HTTP client.
- Elixir streams to automatically retrieve paginated resources.
- Elixir protocols allow easy customization of Dynamo encoding / decoding.
- `mix kinesis.tail your-stream-name` task for easily watching the contents of a kinesis stream.
- Simple. ExAws aims to provide a clear and consistent elixir wrapping around AWS APIs, not abstract them away entirely. For every action in a given AWS API there is a corresponding function within the appropriate module. Higher level abstractions like the aforementioned streams are in addition to and not instead of basic API calls.

## Getting started

Add ex_aws to your mix.exs, along with your json parser and http client of choice. ExAws works out of the box with Poison and :hackney and sweet_xml. All APIs require an http client, but only some require a json or xml codec. You only need the codec for the API you intend to use. At this time only SweetXml is supported for xml parsing.

- Dynamo: json
- Kinesis: json
- Lambda: json
- SQS: xml
- S3: xml

If you wish to use instance roles to obtain AWS access keys you will need to add a JSON codec whether the particular API requires one or not.

```elixir
def deps do
  [
    {:ex_aws, "~> 1.0.0-beta0"},
    {:poison, "~> 2.0"},
    {:hackney, "~> 1.6"}
  ]
end
```
Don't forget to add :hackney to your applications list if that's in fact the http client you choose. `:ex_aws` must always be added to your applications list.

```elixir
def application do
  [applications: [:ex_aws, :hackney, :poison]]
end
```

That's it!

ExAws has by default the equivalent including the following in your mix.exs

```elixir
config :ex_aws,
  access_key_id: [{:system, "AWS_ACCESS_KEY_ID"}, :instance_role],
  secret_access_key: [{:system, "AWS_SECRET_ACCESS_KEY"}, :instance_role]
```

This means it will first look for the AWS standard `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables, and fall back using instance meta-data if those don't exist. You should set those environment variables to your credentials, or configure an instance that this library runs on to have an iam role.


## License

The MIT License (MIT)

Copyright (c) 2014 CargoSense, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
