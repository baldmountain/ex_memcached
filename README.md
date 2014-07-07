# ExMemcached

ExMemcached is a version of memcached written in Elixir. The motivation was mainly as a learning experience. But also, at a previous job, we often had issues with memcached crashing. I thought an OTP based version might hold up a little better.

This version of memcached supports both the binary and ascii protocol and passes most of memcached's own tests. The tests that don't pass mainly have to do with the response to the stats command. Currently ExMemcached doesn't return anything meaningful to the stats command. It doesn't support unix socket or UDP connections or SASL. It doesn't expire items until you ask for them. You can continue to add items to the cache until you crash the machine.

There is still a lot to do before trying to use this in a production environment. The biggest issue at the moment is it seems to not be handling values larger than a few hundred kilo bytes properly. I can't tell if it is an issue with my code or in Erlang/OTP.

I've run ExMemcahced against memslap and memcachetest and they run without error. Just not very fast.

ExMemcached uses ranch for the tcp server, xgen for some OTP stuff, ExLager for logging and exrm for builds.

The license is the MIT license
