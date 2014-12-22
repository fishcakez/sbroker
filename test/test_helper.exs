case :ct.run_test([logdir: 'logs', dir: 'test']) do
  {_, 0, _} -> :ok
  {_, fail, _} -> Mix.raise "Failed #{fail} tests"
end
