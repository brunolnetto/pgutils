FROM postgres:latest

# Copy the custom pg_hba.conf file into the container
COPY pg_hba.conf /var/lib/postgresql/data/pg_hba.conf

# Set appropriate permissions
RUN chown postgres:postgres /var/lib/postgresql/data/pg_hba.conf
