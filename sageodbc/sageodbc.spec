# -*- mode: python -*-

import os

block_cipher = None
package_dir = os.path.abspath(SPECPATH)
project_dir = os.path.dirname(package_dir)


a = Analysis([os.path.join(package_dir, '__main__.py')],
             pathex=[project_dir],
             binaries=None,
             datas=[(os.path.join(package_dir, 'cacert.pem'), '.'),
                    (os.path.join(package_dir, 'templates'), 'templates')],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             cipher=block_cipher)

pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='sageodbc',
          icon=os.path.join(package_dir, 'sageodbc.ico'),
          version=os.path.join(package_dir, 'version.txt'),
          debug=False,
          strip=False,
          upx=True,
          console=True )