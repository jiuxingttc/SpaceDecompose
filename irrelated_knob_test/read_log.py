"""
read the irrelated knob path
"""
import ast

class IrrelatedPathAnalyzer:
    def __init__(self, file_path):
        self.file_path = file_path
        self.configurations = {}
        self.irreal_paths = {}

    def read_file(self):
        # Read configurations from the file
        with open(self.file_path, "r") as file:
            for line in file:
                line = line.strip()
                configuration, prev_configurations = line.split(", ")
                if prev_configurations in self.configurations:
                    self.configurations[prev_configurations].append(line)
                else:
                    self.configurations[prev_configurations] = [line]

    def analyze_irrelated_paths(self):
        path_save = []
        # Iterate through configurations to find irrelated paths
        for prev_configurations, lines in self.configurations.items():
            # if len(lines) == 1:
            path_configurations = {}
            # Extract configurations for each path
            for line in lines:
                line = line.strip().rstrip('}')
                config = line.split(", ")[0].split(": ")[1]
                prev = line.split(", ")[1].split(": ")[1]
                prev_list = prev.strip("[]").split(", ")
                prev_list = [item.strip() for item in prev_list if item.strip()]
                prev_list.append(config)
                path_configurations[prev] = prev_list

            # Save paths configurations
            for prev_configurations, values in path_configurations.items():
                path_list = '[' + ''.join(values) + ']'
                path_configurations[prev_configurations] = path_list
                path_save.append(path_list)

        # Format and analyze paths for intersection
        formatted_list = ['[' + ','.join("'" + s.strip("[]") + "'" for s in string.split(',')) + ']' for string in path_save]
        nested_list = [ast.literal_eval(item) for item in formatted_list]

        for i, value in enumerate(nested_list):
            current_set = set(value)
            disjoint_values = []
            # Check for intersections between paths
            for j, other_value in enumerate(nested_list):
                if i != j:
                    other_set = set(other_value)
                    if current_set.intersection(other_set):
                        continue
                    else:
                        disjoint_values.append(other_value)
            self.irreal_paths[str(value)] = disjoint_values

    def print_irrelated_paths(self):
        # Print irrelated paths
        for path, disjoint_values in self.irreal_paths.items():
            print(f"{path}: {disjoint_values}")
            print()

if __name__ == '__main__':
    # Example usage
    file_path = "/root/AI4DB/irrelated_knob_test/prev_config.log"
    analyzer = IrrelatedPathAnalyzer(file_path)
    analyzer.read_file()
    analyzer.analyze_irrelated_paths()
    analyzer.print_irrelated_paths()



# # init
# configurations = {}

# # read file
# with open("/root/AI4DB/irrelated_knob_test/prev_config.log", "r") as file:
#     for line in file:
#         # 去除行首尾的空白字符
#         line = line.strip()
#         # 提取 configuration 和 prev configurations 的值
#         configuration = line.split(", ")[0]
#         prev_configurations = line.split(", ")[1]


#         # 如果已经存在相同的 prev configurations，则将当前行添加到相应的列表中
#         if prev_configurations in configurations:
#             configurations[prev_configurations].append(line)
#         else:
#             configurations[prev_configurations] = [line]

# # 输出分类后的原始信息
# for prev_configurations, lines in configurations.items():
#     print(prev_configurations + ":")
#     for line in lines:
#         print(line)
#     print()
    
# # print("**********************************************无重复路径1************************************************")
# # for prev_configurations, lines in configurations.items():
# #     if(len(lines)==1):
# #         print(prev_configurations + ":")
# #         for line in lines:
# #             print(line)
# #         print()  # 打印空行以分隔不同的分类

# print("**********************************************输出路径************************************************")
# path_save = []
# for prev_configurations, lines in configurations.items():
#     if(len(lines)==1):
#         print(prev_configurations + ":")
#         # 初始化一个空字典来存储分类后的内容
#         path_configurations = {}
#         # 遍历每行
#         for line in lines:
#             print(line)
#             # 去除行首尾的空白字符
#             line = line.strip().rstrip('}')
#             # 提取 configuration 和 prev configurations 的值
#             config = line.split(", ")[0].split(": ")[1]
#             prev = line.split(", ")[1].split(": ")[1]
#             # 将prev configurations的字符串转换为列表
#             prev_list = prev.strip("[]").split(", ")
#             # 移除空字符串和额外的空格
#             prev_list = [item.strip() for item in prev_list if item.strip()]
#             # 将configuration的值添加到prev configurations的列表中
#             prev_list.append(config)
#             # 将结果添加到字典中
#             path_configurations[prev] = prev_list

#         # 输出分类后的原始信息
#         for prev_configurations, values in path_configurations.items():
#             # print(f"{{path_list: [{''.join(values)}]}}")
#             # 将path_list存储为字符串
#             path_list = '[' + ''.join(values) + ']'
#             # 存储path_list到字典中
#             path_configurations[prev_configurations] = path_list
#             print(f"{{path_list: {path_list}}}")
#             path_save.append(path_list)
#         print()  # 打印空行以分隔不同的分类
# print(path_save)
# print(type(path_save))
# formatted_list = ['[' + ','.join("'" + s.strip("[]") + "'" for s in string.split(',')) + ']' for string in path_save]
# print(type(formatted_list))
# print(formatted_list)
# nested_list = [ast.literal_eval(item) for item in formatted_list]
# print(nested_list)


# print("**********************************************输出不相关路径************************************************")
# # 创建一个空字典来存储路径及其对应的不相交值
# irreal_paths = {}

# # 遍历nested_list，判断每两个值的元素内部是否有交集
# for i, value in enumerate(nested_list):
#     # 将当前值转换为集合
#     current_set = set(value)

#     # 创建一个空列表来存储不相交的值
#     disjoint_values = []

#     # 遍历nested_list中的其他值
#     for j, other_value in enumerate(nested_list):
#         # 如果不是当前值
#         if i != j:
#             # 将其他值转换为集合
#             other_set = set(other_value)
#             if current_set.intersection(other_set):
#                 print("true")
#             else:
#                 print("false")
#                 disjoint_values.append(other_value)

#     # 将不相交的值添加到当前值的列表中
#     irreal_paths[str(value)] = disjoint_values

# # 输出irreal_paths字典
# for path, disjoint_values in irreal_paths.items():
#     print(f"{path}: {disjoint_values}")

# print("##############################################################################")









