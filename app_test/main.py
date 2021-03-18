import tkinter as tk

# 第1步，实例化object，建立窗口window
window = tk.Tk()

# 第2步，给窗口的可视化起名字
window.title('UA补完计划')

# 第3步，设定窗口的大小(长 * 宽)
window.geometry('500x350')  # 这里的乘是小x


# 第4步，在图形界面上设定输入框控件entry框并放置
text_view = tk.Text(window, show=None, width=60, height=10)  # 显示成明文形式
text_view.pack()


# 获取输入字符串
def retrieve_input():
    input_value = text_view.get("0.0", "end-1c")
    return input_value


# 第5步，定义两个触发事件时的函数insert_point和insert_end（注意：因为Python的执行顺序是从上往下，所以函数一定要放在按钮的上面）
def format_ua_firefox():  # 在鼠标焦点处插入输入内容
    t.delete("0.0", "end-1c")
    var = text_view.get("0.0", "end-1c")
    # for line in var:
    #     print(line)
    var_delete = var.split('\n')[0] + '\n'
    var = var.replace(var_delete, "")
    var = "'" + var.replace("\n", "',\n'").replace(": ", "': '") + "',"
    t.insert('insert', var)


# 第6步，创建并放置两个按钮分别触发两种情况
button_1 = tk.Button(window, text='format', width=10, height=2, command=format_ua_firefox)
button_1.pack()

# 第7步，创建并放置一个多行文本框text用以显示，指定height=3为文本框是三个字符高度
t = tk.Text(window, width=60, height=10)
t.pack()


# 第8步，主窗口循环显示
window.mainloop()


'''The first part, "1.0" means that the input should be read from line one, character zero.

The end-1c is divided in 2 parts:

end: Read until the end of the text.
1c: Remove 1 character starting from the end.'''