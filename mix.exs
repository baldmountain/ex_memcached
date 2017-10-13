defmodule ExMemcached.Mixfile do
  use Mix.Project

  def project do
    [ app: :ex_memcached,
      version: "0.0.2",
      elixir: "~> 1.5",
      deps: deps()]
      # elixirc_options: options(Mix.env) ]
  end

  # Configuration for the OTP application
  def application() do
    [
      mod: { ExMemcached, [] },
      applications: [:logger, :ranch], # , :sasl],
      # application configuration goes here:
      env: [ ]
    ]
  end

  # Returns the list of dependencies in the format:
  # { :foobar, git: "https://github.com/elixir-lang/foobar.git", tag: "0.1" }
  #
  # To specify particular versions, regardless of the tag, do:
  # { :barbat, "~> 0.1", github: "elixir-lang/barbat" }
  defp deps() do
    [
      { :ranch, "~> 1.4.0" },
      { :distillery, "~> 1.5", runtime: false }
    ]
  end

  # defp options(env) when env in [:dev, :test] do
  #   [exlager_level: :debug, exlager_truncation_size: 8096]
  # end
  # defp options(env) when env in [:prod] do
  #   [exlager_level: :error, exlager_truncation_size: 8096]
  # end
end
