# Encrypted-SPJ

## What the project is about:

Users may want to use third-party database providers to store confidential information which should not be leaked to an untrusted, third-party server. My project explores a method to allow a subset of SQL operations to be performed on encrypted data, allowing third-party servers to be used with a provably small leakage profile against the threat model of an honest-but-curious adversary. I achieve this leakage through the use of encrypted multimaps (EMMs) as a primitive to precompute and store an encrypted indexing scheme which supports selection, projection, and join operations. I implement two methods to compute joins with differing leakage profiles, amounts of client-side post-processing, and bandwidths returned by the server. To help the client choose between which of these two join implementations they would want to use, I store client-side statistics about the data to provide a method to estimate how much leakage and bandwidth will result from the logically equivalent queries.

In the best case (using only the more secure of the two join methods), my method will leak only the access pattern, the number of rows which match each selection predicate, and the number of rows which will be accessed in each join that is queried. In the worst case, the method will additionally leak the frequencies of each value involved in a join.

## How to run:
I follow the standard method for building and running crates in Rust. Tested using Rust version 1.50.0.

You can run a bare-bones simulation of a third party server from the encrypted-server crate and then connect to that server from a client when running the encryped-client crate.
