Partners:
     Name: George Chassiakos
      UID: 204052193
    Email: georgecha@ucla.edu

     Name: Ray Cothern
      UID: 604161519
    Email: rtcothern@gmail.com

Optimizations:
    The main extra optimizations we applied involve
    the pre-checking of conditions that conflict. For
    example, if the WHERE clause has 'key=5 AND key=9'
    then we automatically know to return no results. This
    also applies for conflicts with the inequality operators.

    The conditions are also checked ahead of time to ensure
    that no values are being read when not necessary.

    Finally, counting is optimized to use the index if the only
    conditions that exist are key-conditions. If the only existing
    conditions are value-conditions, counting does not use the index.
    Mixed cases are chosen on a much more specific basis.
