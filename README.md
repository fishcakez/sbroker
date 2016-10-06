sbroker
=======

`sbroker` is a library that provides the building blocks for creating a pool
and/or a load regulator. The main goals of the library are to minimise upper
percentile latency by smart queuing, easily change the feature set live with
minimal changes, easily inspect a live system and provide confidence with
property based testing.

Documentation
-------------

Documentation is hosted on hex: http://hexdocs.pm/sbroker/


Requirements
------------

The minimum OTP version supported is 18.0.

The `sasl` application is required to start the `sbroker` application. The
`sasl` `error_logger` handler can be disabled by setting the `sasl` application
env `sasl_error_logger` to `false`.

Installing
----------

For rebar3 add `sbroker` as a depencency in `rebar.config`:

```erlang
{deps, [sbroker]}.
```

Other build tools may work if they support `rebar3` dependencies but are not
directly supported.

Testing
-------

```
$ rebar3 ct
```

Smart Queuing
-------------

* Workers are explicitly queued by user code to isolate faults and
  initialisation
* Highly configurable queues with passive queue management for both backpressure
  and loading shedding
* State of the art active queue management algorithms, CoDel and PIE, with
  simple and consistent configuration, to maintain a responsive system during
  overload.
* Load regulated "pool" resizing using queue delay explicitly controlled by user
  code to prevent worker churn.

Changing Features
-----------------

* All features integrate with each other with consistent configuration.
* All processes support live reconfiguration, either automatically as part of a
  release upgrade or explicitly using function calls.
* Code for unused features is not called.

Introspection
-------------

* Meters to view message queue delay, internal queue delay and processing delay.
* Alarms that promotely warn of issues.
* Support for `sys:get_state` and `sys:get_status`.
* Additional calls to view queue lengths and regulator size.
* Queue calls return feedback separating queue delay and processing delay.

Property Testing
----------------

* PropEr is used to property test the majority of features.
* Internal property testing frameworks to easily test new callback modules.

License
-------

This project is licensed under the Apache License, 2.0.


Roadmap
-------

* 1.0 - Improve testing of edge cases (errors) in live reconfiguration
* 1.1 - Add circuit breaker sregulator valves
* 1.2+ - Add improved queue management algorithms when possible, if at all
