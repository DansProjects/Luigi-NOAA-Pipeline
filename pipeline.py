import luigi
import os
import gzip
from urllib.parse import urljoin
from urllib.request import urlopen
from bs4 import BeautifulSoup as Soup


class ScrapeEventDetailsTask(luigi.Task):
    id = luigi.Parameter(default=0)
    url = luigi.Parameter()

    def run(self):

        noaa_repo = urlopen(self.url)
        noaa_html = noaa_repo.read().decode('utf-8')

        bs = Soup(noaa_html, 'html.parser')
        links = bs.findAll('a')

        for link in links:
            if link['href'].endswith('.csv.gz') and link['href'].startswith("StormEvents_details"):
                content = urlopen(urljoin(self.url, link['href']))
                file_name = link.contents[0]
                file = open("results/scraped/{}".format(file_name), "b+w")
                file.write(content.read())
                file.close()

        file = open("results/scrape_complete_{}.txt".format(self.id), "w")
        file.write("DONE!")
        file.close()

    def output(self):
        return luigi.LocalTarget("results/scrape_complete_{}.txt".format(self.id))


class ExtractFilesTask(luigi.Task):
    id = luigi.Parameter(default=0)
    url = luigi.Parameter()
    scraped_path = "results/scraped/"
    extracted_path = "results/extracted/"

    def requires(self):
        return [
            ScrapeEventDetailsTask(self.id, self.url)
        ]

    def run(self):
        for filename in os.listdir(self.scraped_path):
            if filename.endswith('.csv.gz'):
                file = gzip.open(os.path.join(self.scraped_path,filename))
                file_contents = file.read()
                extracted_file = open(os.path.join(self.extracted_path,filename[:-3]), "wb")
                extracted_file.write(file_contents)
                extracted_file.close()
                file.close()

        done_file = open("results/extract_complete_{}.txt".format(self.id), "w")
        done_file.write("DONE!")
        done_file.close()

    def output(self):
        return luigi.LocalTarget("results/extract_complete_{}.txt".format(self.id))


class CreateCombinedDetailsTask(luigi.Task):
    id = luigi.Parameter(default=0)
    url = luigi.Parameter()
    extracted_path = "results/extracted/"
    combined_path = "results/combined_{}.csv".format(id)

    def run(self):

        for filename in os.listdir(self.extracted_path):

            if filename.endswith('.csv'):
                extracted_file = open(os.path.join(self.extracted_path,filename), "r")
                combined_file = open(self.combined_path, "a+")

                if os.path.getsize(self.combined_path) > 0:
                    # file is not empty, skip header row
                    extracted_file.readline()

                extracted_file_contents = extracted_file.read()
                combined_file.write(extracted_file_contents)

    def requires(self):
        return [
            ExtractFilesTask(self.id, self.url)
        ]

    def output(self):
        return luigi.LocalTarget("results/combined_{}.csv".format(self.id))


class NOAATask(luigi.Task):
    id = luigi.Parameter(default=0)
    url = luigi.Parameter()

    def run(self):
        # maybe do some fun stuff here
        return

    def requires(self):
        return [
            CreateCombinedDetailsTask(self.id, self.url)
        ]

    def output(self):
        return luigi.LocalTarget("NOAATask")


if __name__ == '__main__':
    luigi.run(
        ["--local-scheduler",
         "--url=https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/"],
        main_task_cls=NOAATask,
  )
