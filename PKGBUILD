_name="ebt"
_module="ebt"

pkgname=("python-${_module}")
pkgver="0.65"
pkgrel="1"
pkgdesc="Enchanced backup tool"
arch=("any")
url="https://bitbucket.org/uMzwX8An9-kG/${_name}"
license=("GPL")
depends=('python-configobj' 'rsync' 'btrfs-progs')
source=("https://github.com/larrabee/ebt/archive/${pkgver}.zip")
backup=('etc/ebt/ebt.conf' 'etc/ebt/plans.py')
md5sums=('1d032313a9f2c7fd7a2d5072ef4942c7')

prepare() {
  echo "prepare"
}

package() {
  mkdir -p ${pkgdir}/usr/lib/python3.4/site-packages/ebt/
  mkdir -p ${pkgdir}/etc/ebt
  mkdir -p ${pkgdir}/usr/bin
  cp -r ${srcdir}/ebt-${pkgver}/code/* ${pkgdir}/usr/lib/python3.4/site-packages/ebt/
  cp -r ${srcdir}/ebt-${pkgver}/conf/* ${pkgdir}/etc/ebt/
  ln -s /usr/lib/python3.4/site-packages/ebt/ebt.py ${pkgdir}/usr/bin/ebt
}

