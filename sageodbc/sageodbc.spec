# -*- mode: python -*-

block_cipher = None


a = Analysis(['cli.py'],
             pathex=['D:\\Paul\\Documents\\GitHub\\sage-odbc\\sageodbc'],
             binaries=None,
             datas=[('cacert.pem', '.')],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='sageodbc',
          icon='sageodbc.ico',
          version='version.txt',
          debug=False,
          strip=False,
          upx=True,
          console=True )