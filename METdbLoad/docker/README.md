Sample command to run Dockerfile:

docker run -v {xmldir}/name.xml:/data/name.xml -v {datadir}:{datadir} -v {dir}:/data/tmp -p 3306:3306 dbdock /data/name.xml /data/tmp

- Puts the XML file into a Docker data directory
- Creates a Docker volume with the same name as the local data directory used in the XML file
- Links a local directory to a Docker /data/tmp directory for the output data files
- Gives the name dbdock to the image
- Passes in the Docker data directory with the name of the XML load file to the Python program
- Passes in the Docker /data/tmp directory to the Python program as the location for the output data files
- Uses port 3306 for MySQL. This must match the XML load file and the exposed port in the Dockerfile
- The XML load file has the host, username, and password information

