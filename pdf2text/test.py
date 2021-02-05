from pdf2docx import Converter

pdf_file = 'D:\code\pdf/yy.pdf'
docx_file = 'D:\code\pdf/yy.docx'

# convert pdf to docx
cv = Converter(pdf_file)
cv.convert(docx_file, start=0, end=None)
cv.close()