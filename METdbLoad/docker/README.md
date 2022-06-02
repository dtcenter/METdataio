Sample command to run Dockerfile for a pre-existing MySQL database:

docker run -v {xmldir}/name.xml:/data/name.xml -v {datadir}:/data/temp -v {dir}:/data/tdat -p 3306:3306 dbdock /data/name.xml /data/temp

- Puts the XML load file into a Docker data directory
- Creates a Docker volume for the data directory used in the XML file
- Links a local directory to a Docker /data/tdat directory for the output data files
- Gives the name dbdock to the image (must match the docker build name)
- Passes in the Docker data directory with the name of the XML load file to the Python program
- Passes in the Docker /data/temp directory to the Python program as the location for the output data files
- Uses port 3306 for MySQL. This must match the XML load file and the exposed port in the Dockerfile
- The XML load file has the host, username, and password information

{xmldir} is the local directory on your computer that contains the XML file
{datadir} is a local temp directory for output data files
/data/temp is a temp directory under /data in the container (appears twice)
{dir} is a local directory containing the data to load
/data/tdat is a directory on the container that will link to the data -
this is the directory name that must be used in the XML file for the data

NOTE: On a MAC OS laptop, 2M was not enough memory for Docker - increased to 4M.
