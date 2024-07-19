import pandas
import pandas as pd
import os
import zipfile


class Convertor:
    def _read_file(self, file):
        if file.endswith('.csv'):
            df = pd.read_csv(file, sep=';', dtype=str)
        elif file.endswith('.xlsx') or file.endswith('.xls'):
            df = pandas.read_excel(file, dtype=str)
        return df

    def zipDir(self, dirpath, outFullName):
        zip = zipfile.ZipFile(outFullName, "w", zipfile.ZIP_DEFLATED)
        for path, dirnames, filenames in os.walk(dirpath):
            # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
            fpath = path.replace(dirpath, '')

            for filename in filenames:
                zip.write(os.path.join(path, filename), os.path.join(fpath, filename))
        zip.close()

    def convert_to_excel(self, file, output_dir):
        df = self._read_file(file)
        df.to_excel(output_dir, index=False)

    def convert_to_csv(self, file, output_dir):
        df = self._read_file(file)
        df.to_csv(output_dir, sep=';', index=False)

    def batch_generate_csv_pack_and_excel_pack(self, folder_dir):

        csv_output_dir = folder_dir + ' csv版本/'
        excel_output_dir = folder_dir + ' excel版本/'
        folder_dir = folder_dir + '/'
        os.makedirs(csv_output_dir)
        os.makedirs(excel_output_dir)
        for root, dirs, files in os.walk(folder_dir):
            for item in files:
                print('正在处理' + item + '...')
                if item.endswith('.xlsx') or item.endswith('.xls'):
                    if item.endswith('.xlsx'):
                        file_name = item[:-5]
                    elif item.endswith('.xls'):
                        file_name = item[:-4]
                elif item.endswith('.csv'):
                    file_name = item[:-4]
                self.convert_to_csv(folder_dir + item, csv_output_dir + file_name + '.csv')
                self.convert_to_excel(folder_dir + item, excel_output_dir + file_name + '.xlsx')

# if __name__ == '__main__':
#     convertor = Convertor()
#     # original_file = r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\stations_dictionary.csv"
#     # output_dir = r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\stations_dictionary.xlsx"
#     # convertor.convert_to_excel(original_file, output_dir)
#
#     folder_to_be_convert = r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\北京MAM L跑数文件"
#     convertor.batch_generate_csv_pack_and_excel_pack(folder_to_be_convert)
