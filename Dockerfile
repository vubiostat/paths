FROM rocker/verse:latest
RUN apt-get update && \
    apt-get install -y libssl-dev libcurl4-openssl-dev libsodium-dev libsecret-1-dev libudunits2-dev libgdal-dev libgeos-dev libproj-dev && \
    rm -rf /var/lib/apt/lists/*
RUN R -e "options(warn=2); install.packages(c('redcapAPI', 'keyring', 'sf', 'httr2', 'emayili', 'knitr', 'markdown'))"
ADD "https://api.github.com/repos/vubiostat/redcapAPI/tarball" redcapAPI.tar.gz
RUN R CMD INSTALL redcapAPI.tar.gz
WORKDIR /app
COPY . .
ENTRYPOINT ["Rscript", "email-proof-of-concept.R"]
