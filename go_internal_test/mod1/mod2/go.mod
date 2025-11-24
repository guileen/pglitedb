module example.com/othermodule

go 1.21

replace example.com/internaltest => ../..

require example.com/internaltest v0.0.0-00010101000000-000000000000
