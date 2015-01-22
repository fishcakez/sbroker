defmodule SBroker.Mixfile do
    use Mix.Project

      def project do
        [app: :sbroker,
          version: "0.3.0-dev",
          elixir: "~> 1.0",
          deps: deps(),
          source_url: "https://github.com/fishcakez/sbroker",
          description: "Sojourn-time based active queue management process
            broker",
          package: package(),
          aliases: [docs: "edoc"]]
      end

      def application, do: []

      def deps() do
        [{:"mix-erlang-tasks", github: "alco/mix-erlang-tasks", only: :dev},
          {:proper, github: "manopapad/proper", tag: "v1.1", only: :test}]
      end

      defp package do
        [contributors: ["James Fish"],
          licenses: ["Apache v2.0"],
          links: %{"Github" => "https://github.com/fishcakez/sbroker"}]
      end

end
