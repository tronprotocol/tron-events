const {keccak256} = require('js-sha3');

module.exports = (req, res, next) => {

  const hash = process.env.NODE_ENV
      ? 'b3c435a0f5053a42e39ac41c4ba59fb53d867d27a2e86fcf8f37e1c0b76e8369'
      : '09d1fc7ac728e5083050620b502305161f10c966651ddc41e0399225e4029370'

  if (keccak256(req.get('secret')).toString() === hash) {
    return next()
  } else {
    res.json({
      success: false,
      error: 'Access not authorized.'
    })
  }
}