defmodule KinesisClient.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kinesis_client,
      version: "1.1.24",
      elixir: "~> 1.16.0-otp-26",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      package: package(),
      description: description(),
      deps: deps(),
      source_url: "https://github.com/uberbrodt/kcl_ex",
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  def description do
    """
    A pure Elixir implementation of the AWS Java Kinesis Client Library (KCL)
    """
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      licenses: ["Apache 2.0"],
      maintainers: ["Chris Brodt"],
      links: %{Github: "https://github.com/uberbrodt/kcl_ex"}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.1.0"},
      {:configparser_ex, "~> 4.0"},
      {:credo, "~> 1.0", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:ecto_sql, "~> 3.6"},
      {:ex_aws, "~> 2.1"},
      {:ex_aws_dynamo, "~> 4.0"},
      {:ex_aws_kinesis, "~> 2.0"},
      {:excoveralls, "~> 0.10", only: :test},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:hackney, "~> 1.9"},
      {:jason, "~> 1.1"},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:mox, "~> 1.0", only: :test},
      {:retry, "~> 0.14"},
      {:sobelow, "~> 0.13", only: [:dev, :test], runtime: false},
      {:styler, "~> 0.7", only: [:dev, :test], runtime: false}
    ]
  end
end
