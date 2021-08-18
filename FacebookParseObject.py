import codecs
import json
import re
import pendulum


class FacebookObjectSummarize(object):
    def __init__(self, tree):
        self.location = None
        self.age = None
        self.gender = 100
        self.param_about = "/about"
        self.param_detail = "/about_contact_and_basic_info"
        self.tree = tree

    def get_page_location(self):
        """Page location"""
        location_tree = self.tree.xpath("//*[contains(text(), 'full_address')]//text()")
        try:
            self.location = re.findall(r'(?=full_address)(.*)(?="},"page)', location_tree[0])[0].replace(
                'full_address":"', "")

            if len(self.location) > 1000 and "addressEditable" in self.location:
                self.location = self.location.split('"},"')[0]

            if "\\" in self.location:
                self.location = codecs.decode(self.location, 'unicode_escape')
            else:
                self.location = self.location.encode("utf-8").decode("utf-8")
        except IndexError:
            pass

    def get_group_location(self):
        """Group location"""
        try:
            xpath_location = self.tree.xpath("//*[contains(text(), 'group_locations')]//text()")
            match_location = re.findall(r'(?=group_locations)(.*)(?="__typename":"Page"}])', xpath_location[0])
            if len(match_location) == 0:
                match_location = \
                    re.findall(r'(?=group_locations":)(.*)(?=,"description_with_entities)', xpath_location[0])[
                        0]
            else:
                match_location = match_location[0]
            try:
                dic = json.loads(match_location.replace('group_locations":', "") + '"__typename":"Page"}]')
            except:
                try:
                    match_location = match_location.replace('group_locations":', "")
                    dic = json.loads(match_location.replace('group_locations":', ""))
                except:
                    return self.location
            self.location = dic[0].get("name")
            if "\\" in self.location:
                self.location = codecs.decode(self.location, 'unicode_escape')
            else:
                self.location = self.location.encode("utf-8").decode("utf-8")
        except IndexError:
            pass
        return self.location

    def get_profile_location(self):
        """Profile location"""
        try:
            xpath_current_location = self.tree.xpath(
                "//*[contains(text(), 'CITY_WITH_ID') or contains(text(), 'hometown')]//text()")
            if len(xpath_current_location) > 1:
                for text in xpath_current_location:
                    if "friends_hometown" not in text or "friends_current_city" not in text:
                        xpath_current_location = [text]
            self.location = \
                xpath_current_location[0].split('"},"field_type":"current_city"')[0][-100:].split('"text":')[1].replace(
                    '"',
                    "")
            # try:
            #     match_location = re.findall(r'(.*)(?="},"field_type":"current_city")', xpath_current_location[0])[0]
            # except:
            #     match_location = re.findall(r'(.*)(?="},"field_type":"hometown")', xpath_current_location[0])[0]
            # array_match = match_location[-100:]
            # self.location = array_match.split('"text":')[1].replace('"', "")
            if "\\" in self.location:
                self.location = codecs.decode(self.location, 'unicode_escape')
            else:
                self.location = self.location.encode("utf-8").decode("utf-8")
        except Exception:
            pass
        return self.location

    def get_profile_age(self):
        """Profile age"""
        try:
            xpath_age = self.tree.xpath("//*[contains(text(), 'birthday') "
                                        "and contains(text(), 'delight_ranges')]//text()")
            birthday = int(re.findall(r'(.*)(?="},"field_type":"birthday")', xpath_age[0])[0][-4:])
            self.age = pendulum.now().year - birthday
        except Exception:
            pass
        return self.age

    def get_profile_gender(self):
        """Profile gender"""
        try:
            xpath_gender = self.tree.xpath(
                "//*[contains(text(), 'gender') and contains(text(), 'is_viewer_friend')]//text()")
            gender = re.findall(r'(?="gender":")(.*)(?="})', xpath_gender[0])[0].split(",")[0].replace('"gender":"',
                                                                                                       "").replace('"',
                                                                                                                   "")
            self.gender = gender
        except Exception:
            pass
        return self.gender

    def object_sum(self):
        self.get_group_location()
        self.get_page_location()
        self.get_profile_gender()
        self.get_profile_location()


def return_region(df, each_loc):
    each_loc = each_loc.split(", ") if "," in each_loc else [each_loc]
    for each_pro, each_pron, each_dis, each_disn in zip(df["Provinces"].tolist(), df["Provinces_no"].tolist(),
                                                        df["Districts"].tolist(), df["Districts_no"].tolist()):
        for each_p in each_loc:
            if each_p in str(each_dis):
                try:
                    return df["Provinces"][df["Districts"] == each_dis].tolist()[0], \
                           df["Central"][df["Districts"] == each_dis].tolist()[0], df["Provinces"][
                               df["Provinces"] == df["Provinces"][df["Districts"] == each_dis].tolist()[
                                   0]].index.values.astype(int)[0]
                except:
                    pass
            if each_p in str(each_disn):
                province = df["Provinces"][df["Districts_no"] == each_disn].tolist()[0]
                central = df["Central"][df["Provinces"] == province].tolist()[0]
                code = df["Provinces"][df["Provinces"] == province].index.values.astype(int)[0]
                return province, central, code
