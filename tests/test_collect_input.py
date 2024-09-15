from unittest import TestCase

from dags.collect_input import fetch_md_files, process_md_files, upload_to_github

class Test(TestCase):
    def test_fetch_md_files(self):
        md_files = fetch_md_files()

        process_md_files(md_files=md_files)
        # self.fail()

    def test_upload_to_github(self):
        upload_to_github()