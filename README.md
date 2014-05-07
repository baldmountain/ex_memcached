# ExMemcached

ExMemcached is a version of memcached written in Elixir. The motivation was mainly as a learning experience. But also, while I was working at RueLala, we often had issues with memcached crashing. I thought an OTP based version might hold up a little better.

This version of memcached supports both the binary and ascii protocol and passes most of memcached's own tests. The tests that don't pass mainly have to do with the response to the stats command. Currently ExMemcached doesn't return anything meaningful to the stats command. It doesn't support unix socket or UDP connections or SASL. It doesn't expire items until you ask for them. You can continue to add items to the cache until you crash the machine.

There is still a lot to do before trying to use this in a production environment.

I've run ExMemcahced against memslap and memcachetest and they run without error. Just not very fast.

ExMemcached uses ranch for the tcp server, xgen for some OTP stuff, ExLager for logging and exrm for builds.

The license is the MIT license
