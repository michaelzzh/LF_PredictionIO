UPDATE pio_meta_enginemanifests
DECLARE
var home varchar;
exec dbms_system.get_env('HOME', :home)
SET files = REPLACE(files, '/home/dev', home)
